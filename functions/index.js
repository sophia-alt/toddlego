const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const axios = require("axios");
const { Client } = require("@googlemaps/google-maps-services-js");
const crypto = require("crypto");
const geofire = require("geofire-common");

// Define the secrets for API keys
const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");
const GOOGLE_MAPS_API_KEY = defineSecret("GOOGLE_MAPS_API_KEY");
const SERPER_DEV_API_KEY = defineSecret("SERPER_DEV_API_KEY");

// Initialize the Google Maps Client
const mapsClient = new Client({});

admin.initializeApp();
const db = admin.firestore();

/**
 * Helper to check if cached coordinates are still fresh (less than 6 months old)
 */
const isCoordinatesCacheFresh = (timestamp) => {
    if (!timestamp) return false;
    const sixMonthsAgo = Date.now() - 180 * 24 * 60 * 60 * 1000;
    return timestamp > sixMonthsAgo;
};

/**
 * Dynamically fetches coordinates for any venue string using Google Maps.
 * Uses a local Firestore cache to minimize API calls.
 * @param {string} venueName - The name of the library or place.
 * @param {string} apiKey - Your Google Maps API Key.
 * @returns {Promise<Object>} { lat, lng, formattedAddress }
 */
async function getDynamicCoordinates(venueName, apiKey) {
    if (!venueName) return { lat: null, lng: null, address: null };

    // Check cache first
    const cacheId = Buffer.from(venueName.toLowerCase()).toString("base64").substring(0, 50);
    const cacheRef = db.collection("geocoding_cache").doc(cacheId);
    const cacheDoc = await cacheRef.get();

    if (cacheDoc.exists) {
        const cached = cacheDoc.data();
        if (isCoordinatesCacheFresh(cached.cachedAt)) {
            console.log(`✅ Cache hit for ${venueName}`);
            return {
                lat: cached.lat,
                lng: cached.lng,
                address: cached.address,
            };
        }
    }

    // Cache miss or expired - call API
    try {
        console.log(`🔎 Geocoding miss for ${venueName}`);

        // Add rate limiting: delay before geocoding to avoid hitting API limits
        await new Promise(resolve => setTimeout(resolve, 200)); // 200ms delay

        // First try with California constraint
        let response = await mapsClient.geocode({
            params: {
                address: `${venueName}, California`,
                key: apiKey,
            },
        });

        // If no results with California constraint, try without it (fallback)
        if (response.data.results.length === 0) {
            console.warn(
                `⚠️ No results for "${venueName}, California" - retrying without location constraint`,
            );
            response = await mapsClient.geocode({
                params: {
                    address: venueName,
                    key: apiKey,
                },
            });
        }

        if (response.data.results.length > 0) {
            const result = response.data.results[0];
            const address = result.formatted_address;
            const lat = result.geometry.location.lat;
            const lng = result.geometry.location.lng;
            const inCaliforniaArea =
                lat >= 32.5 && lat <= 42 && lng <= -114 && lng >= -124;

            if (!inCaliforniaArea) {
                console.warn(
                    `⚠️ Geocoded address is likely outside California: ${address}`,
                );
            }

            // Store in cache
            await cacheRef.set(
                {
                    venueName: venueName,
                    lat: lat,
                    lng: lng,
                    address: address,
                    cachedAt: Date.now(),
                },
                { merge: true }
            );

            console.log(`📍 Geocoded ${venueName}: ${address}`);
            return {
                lat: lat,
                lng: lng,
                address: address,
            };
        }
    } catch (error) {
        console.error(
            `[Geocoding Error] Could not find: ${venueName}`,
            error.message,
        );
    }

    return { lat: null, lng: null, address: null };
}

/**
 * Helper to create a unique, URL-safe ID for each event.
 * Using Title + Venue + Date ensures recurring events are saved separately.
 */
const generateEventId = (title, venue, date) => {
    const rawStr = `${title}-${venue}-${date}`.toLowerCase().replace(/\s+/g, "-");
    return Buffer.from(rawStr).toString("base64").substring(0, 50);
};

// Normalize various age labels into consistent ranges for the client
const normalizeAgeRange = (input) => {
    if (!input) return null;
    const s = String(input).toLowerCase();
    if (/baby|babies|infant/.test(s)) return "0-18 months";
    if (/toddler/.test(s)) return "18-36 months";
    if (/preschool/.test(s)) return "3-5 years";
    if (/all ages/.test(s)) return "All";
    const match = s.match(/(\d+)\s*-\s*(\d+)\s*(months|month|years|year|y)/);
    if (match) {
        const start = match[1];
        const end = match[2];
        const unit = /month/.test(match[3]) ? "months" : "years";
        return `${start}-${end} ${unit}`;
    }
    return null;
};

// Helper to check if an ISO date is in the past
const isPastIsoDate = (iso) => {
    const t = Date.parse(iso);
    if (isNaN(t)) return true;
    return t < Date.now();
};

// Helper to generate SHA-256 hash of content
const generateContentHash = (content) => {
    return crypto.createHash("sha256").update(content, "utf8").digest("hex");
};

/**
 * Clean markdown content by removing dynamic elements before hashing.
 * This prevents false cache misses from timestamps, ads, session IDs, etc.
 */
const cleanContentForHashing = (markdown) => {
    // Remove common dynamic patterns that change on every page load
    let cleaned = markdown
        // Remove timestamps and dates (various formats)
        .replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/g, "")
        .replace(/\d{1,2}\/\d{1,2}\/\d{2,4}/g, "")
        .replace(
            /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b/gi,
            "",
        )
        // Remove "Last updated" sections
        .replace(/last\s+updated:?\s*[^\n]*/gi, "")
        .replace(/updated\s+on:?\s*[^\n]*/gi, "")
        // Remove session/tracking IDs (common patterns)
        .replace(/sessionid[=:]\s*[a-zA-Z0-9]+/gi, "")
        .replace(/trackingid[=:]\s*[a-zA-Z0-9]+/gi, "")
        .replace(/\b[a-f0-9]{32,64}\b/g, "") // MD5/SHA hashes
        // Remove query parameters that might be dynamic
        .replace(/\?[^\s\]]+/g, "")
        // Normalize whitespace
        .replace(/\s+/g, " ")
        .trim();

    return cleaned;
};

/**
 * Main Parsing Function: Daily Worker
 * Loops through all registered libraries in url_registry
 * Checks cache and only calls Gemini if content has changed
 * ENABLED: 2025-01-XX
 */
exports.dailyLibraryScraper = onSchedule(
    {
        schedule: "every 24 hours",
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 300,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🚀 Starting Daily Library Scraper...");


        // Get all registered libraries from Discovery step
        const registrySnap = await db.collection("url_registry").get();

        if (registrySnap.empty) {
            console.log(
                "ℹ️ No libraries in registry. Run discovery function first.",
            );
            return;
        }

        console.log(
            `📚 Processing ${registrySnap.size} registered library websites...`,
        );

        let totalProcessed = 0;
        let totalEventsAdded = 0;

        // Limit to first 50 libraries per run to prevent timeout and excessive API usage
        // Process remaining libraries in next run
        const maxLibrariesPerRun = 50;
        const librariesToProcess = registrySnap.docs.slice(0, maxLibrariesPerRun);

        if (registrySnap.size > maxLibrariesPerRun) {
            console.log(`⚠️ Limiting to ${maxLibrariesPerRun} libraries per run (${registrySnap.size} total). Remaining will be processed in next run.`);
        }

        for (const registryDoc of librariesToProcess) {
            const libraryData = registryDoc.data();
            const targetUrl = libraryData.url_hash;
            const venueName = libraryData.venue_name || "Library";

            // Skip only obviously invalid URLs
            if (!targetUrl) {
                console.log(`⏭️ Skipping empty URL for ${venueName}`);
                continue;
            }

            totalProcessed++;
            console.log(
                `\n🔄 Processing: ${venueName}\n   URL: ${targetUrl.substring(0, 60)}...`,
            );

            try {
                // 1. Fetch rendered content via Jina Reader
                // Add rate limiting: delay between requests to avoid overwhelming Jina API
                if (totalProcessed > 1) {
                    await new Promise(resolve => setTimeout(resolve, 2000)); // 2 second delay between libraries
                }

                const readerUrl = `https://r.jina.ai/${targetUrl}`;
                const fetchResponse = await fetch(readerUrl);

                if (!fetchResponse.ok) {
                    console.warn(
                        `⚠️ Failed to fetch ${venueName}: ${fetchResponse.statusText}`,
                    );
                    continue;
                }

                const markdown = await fetchResponse.text();
                console.log(`✅ Fetched ${venueName}`);

                // 2. Check cache
                const contentToAnalyze = markdown.substring(0, 40000);
                const cleanedContent = cleanContentForHashing(contentToAnalyze);
                const currentHash = generateContentHash(cleanedContent);

                const urlDocId = registryDoc.id;
                const cacheRef = db.collection("url_registry").doc(urlDocId);
                const cacheDoc = await cacheRef.get();

                // Cache hit - skip Gemini
                if (cacheDoc.exists && cacheDoc.data().content_hash === currentHash) {
                    const lastParsed = new Date(
                        (cacheDoc.data().last_parsed || 0) * 1000,
                    ).toLocaleString();
                    console.log(
                        `✅ Cache Hit! Skipping Gemini. (Last parsed: ${lastParsed})`,
                    );
                    continue;
                }

                console.log("🔄 Cache Miss - Calling Gemini for analysis...");

                // 3. Initialize Gemini
                const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
                const model = genAI.getGenerativeModel({
                    model: "gemini-2.0-flash",
                    generationConfig: {
                        responseMimeType: "application/json",
                    },
                });

                // 4. AI Analysis
                const prompt = `
        You are a specialized data extraction engine for "Toddlego," an app helping parents find activities for children aged 0-4.
        Your task is to parse the provided markdown text from a library or community website and extract specific toddler-focused events.

        ### 1. TARGET AUDIENCE & FILTERING
        - ONLY extract events explicitly for: Babies (0-18m), Toddlers (18-36m), or Preschoolers (3-5y).
        - INCLUSION KEYWORDS: Storytime, Play & Learn, Music & Movement, Baby Bounce, Stay & Play, Tiny Tots.
        - EXCLUSION RULES: 
            - Strictly EXCLUDE: Teens, Adults, "School-age", "Grades K-5", or "Tweens".
            - ONLY include "Family" events if the description explicitly mentions "toddlers", "babies", or "all ages including littles".

        ### 2. DATE & TIME PROCESSING (CRITICAL)
        - Source text may use relative dates (e.g., "Tomorrow", "Next Wednesday") or specific dates (e.g., "Dec 25").
        - Assume the current reference year is 2025 unless the text says otherwise.
        - Convert all dates to valid ISO 8601 format (YYYY-MM-DDTHH:mm:ss).
        - If multiple sessions exist for one event, create a separate entry for each date/time.

        ### 3. VENUE & LOCATION LOGIC
        - Library systems often have many branches. Look for the branch name (e.g., "Union City Branch", "Centerville Library").
        - If the branch name is missing from the event card, look for it in the page headers or site navigation text provided.
        - Always provide the FULL official name (e.g., "Alameda County Library - Union City Branch") to ensure geocoding works.

        ### 4. DATA MODEL & JSON SCHEMA
        Return a JSON object with a key "events" containing an array of objects. 
        Use "null" for missing optional fields.

        {
        "events": [
            {
            "title": "Short, clear event title",
            "venue": "Full official library system and branch name",
            "description": "A warm, helpful 2-sentence summary. Highlight sensory details (bubbles, music, building blocks).",
            "isoDate": "YYYY-MM-DDTHH:mm:ss",
            "ageRange": "Identify the target age (e.g., '0-2 years', 'Toddlers', 'All Ages')",
            "isRegistrationRequired": boolean,
            "registrationUrl": "Direct link to sign up if found, else null",
            "isIndoor": true
            }
        ]
        }

        ### 5. CONTENT TO ANALYZE
        ${contentToAnalyze}
    `;

                const result = await model.generateContent(prompt);
                const aiResponse = JSON.parse(result.response.text());
                const extractedEvents = aiResponse.events || [];

                console.log(
                    `🤖 Gemini found ${extractedEvents.length} relevant events`,
                );

                if (extractedEvents.length === 0) {
                    console.log(`ℹ️ No toddler events found for ${venueName}`);
                    // Update cache
                    await cacheRef.set(
                        {
                            content_hash: currentHash,
                            last_parsed: Math.floor(Date.now() / 1000),
                            event_count: 0,
                            parsed_json: JSON.stringify([]),
                        },
                        { merge: true },
                    );
                    continue;
                }

                // 5. Batch Upload with Deduplication
                const batch = db.batch();
                let newEventsCount = 0;

                for (const act of extractedEvents) {
                    if (!act || !act.title || !act.venue || !act.isoDate) {
                        console.warn("⚠️ Skipping invalid event:", act);
                        continue;
                    }

                    const parsed = Date.parse(act.isoDate);
                    if (Number.isNaN(parsed)) {
                        console.warn("⚠️ Skipping event with invalid isoDate:", act);
                        continue;
                    }

                    // Skip past events
                    if (isPastIsoDate(act.isoDate)) {
                        continue;
                    }

                    const eventDate = String(act.isoDate).split("T")[0];
                    const uniqueId = generateEventId(
                        String(act.title),
                        String(act.venue),
                        eventDate,
                    );

                    const docRef = db.collection("activities").doc(uniqueId);
                    const docSnap = await docRef.get();

                    // Only add if new
                    if (!docSnap.exists) {
                        const mapsKey =
                            GOOGLE_MAPS_API_KEY &&
                                typeof GOOGLE_MAPS_API_KEY.value === "function"
                                ? GOOGLE_MAPS_API_KEY.value()
                                : process.env.GOOGLE_MAPS_API_KEY;

                        // Use pre-discovered coordinates if available, else geocode
                        const coordinates =
                            libraryData.latitude && libraryData.longitude
                                ? {
                                    lat: libraryData.latitude,
                                    lng: libraryData.longitude,
                                    address: libraryData.venue_name,
                                }
                                : await getDynamicCoordinates(act.venue, mapsKey);

                        const geohash =
                            coordinates.lat && coordinates.lng
                                ? geofire.geohashForLocation([
                                    coordinates.lat,
                                    coordinates.lng,
                                ])
                                : null;

                        const normalized = {
                            title: String(act.title || "").trim(),
                            venue: String(act.venue || "").trim(),
                            description: act.description ?? null,
                            startTime: Math.floor(
                                new Date(act.isoDate).getTime() / 1000,
                            ),
                            // Ensure a reasonable end time if missing (default 1 hour after start)
                            endTime: act.endTime
                                ? Math.floor(new Date(act.endTime).getTime() / 1000)
                                : Math.floor(new Date(act.isoDate).getTime() / 1000) + 60 * 60,
                            ageRange: normalizeAgeRange(act.ageRange) ?? "All",
                            isFree: true,
                            requiresBooking: !!act.isRegistrationRequired,
                            registrationUrl:
                                act.registrationUrl &&
                                    /^https?:\/\//.test(act.registrationUrl)
                                    ? act.registrationUrl
                                    : null,
                            latitude: coordinates.lat,
                            longitude: coordinates.lng,
                            geohash: geohash,
                            position:
                                coordinates.lat && coordinates.lng
                                    ? {
                                        geohash: geohash,
                                        geopoint: new admin.firestore.GeoPoint(
                                            coordinates.lat,
                                            coordinates.lng,
                                        ),
                                    }
                                    : null,
                            sourceUrl: targetUrl,
                            createdAt: Math.floor(Date.now() / 1000),
                            // Expire shortly after the event ends to avoid showing stale entries.
                            // If no endTime provided, expire 2 hours after start.
                            expireAt: new Date(
                                (act.endTime
                                    ? new Date(act.endTime).getTime()
                                    : new Date(act.isoDate).getTime() + 2 * 60 * 60 * 1000) +
                                5 * 60 * 1000 // small buffer
                            ),
                            // New structured schema fields (dual-write for compatibility)
                            type: "one_time",
                            category: "Library Program",
                            location:
                                coordinates.lat && coordinates.lng
                                    ? {
                                        name: String(act.venue || venueName || "").trim(),
                                        geohash: geohash,
                                        geopoint: new admin.firestore.GeoPoint(
                                            coordinates.lat,
                                            coordinates.lng,
                                        ),
                                    }
                                    : {
                                        name: String(act.venue || venueName || "").trim(),
                                    },
                            timing: {
                                is_all_day: false,
                                start_time: Math.floor(new Date(act.isoDate).getTime() / 1000),
                                end_time:
                                    act.endTime
                                        ? Math.floor(new Date(act.endTime).getTime() / 1000)
                                        : Math.floor(new Date(act.isoDate).getTime() / 1000) + 60 * 60,
                                recurrence: null,
                                business_hours: null,
                            },
                            age_range:
                                (() => {
                                    const s = normalizeAgeRange(act.ageRange);
                                    const m = s && s.match(/(\d+)\s*-\s*(\d+)/);
                                    if (m) return [Number(m[1]), Number(m[2])];
                                    return null;
                                })(),
                            source: "web_scrape",
                            tags: [],
                        };

                        batch.set(docRef, normalized);
                        newEventsCount++;
                    }
                }

                if (newEventsCount > 0) {
                    await batch.commit();
                    totalEventsAdded += newEventsCount;
                    console.log(
                        `✅ Added ${newEventsCount} new events from ${venueName}`,
                    );
                }

                // Update cache
                await cacheRef.set(
                    {
                        content_hash: currentHash,
                        last_parsed: Math.floor(Date.now() / 1000),
                        event_count: newEventsCount,
                        parsed_json: JSON.stringify(extractedEvents),
                    },
                    { merge: true },
                );
            } catch (error) {
                console.error(`❌ Error processing ${venueName}:`, error.message);
            }
        }

        console.log(
            `\n🎉 Daily Scraper Complete:\n   Processed: ${totalProcessed} libraries\n   Events Added: ${totalEventsAdded}`,
        );
    },
);

/**
 * Discovery Function: Monthly Scout
 * Searches for public libraries in California cities using Google Places API
 * Stores discovered library websites in url_registry for the daily scraper to process
 * ENABLED: 2025-01-XX
 */
exports.discoverCaliforniaLibraries = onSchedule(
    {
        schedule: "0 0 1 * *", // Runs once a month (1st day at midnight UTC)
        secrets: [GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 120,
        memory: "256MiB",
    },
    async (event) => {
        console.log("🔍 Starting California Library Discovery (config_cities)...");

        // Pull cities from config_cities collection
        const citiesSnap = await db.collection("config_cities").get();
        if (citiesSnap.empty) {
            console.log("ℹ️ No cities found in config_cities. Seed with import_ca_cities.py.");
            return;
        }

        let totalDiscovered = 0;
        let totalRegistered = 0;

        for (const cityDoc of citiesSnap.docs) {
            const cityRef = cityDoc.ref;
            const cityData = cityDoc.data() || {};
            const cityName = cityData.name || `${cityDoc.id.replace(/_/g, " ")}, CA`;

            console.log(`🔍 Searching for libraries in ${cityName}...`);

            // Mark scanning start
            await cityRef.set(
                {
                    status: "scanning",
                    last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                },
                { merge: true }
            );

            try {
                const queries = [
                    `public library in ${cityName}`,
                    `park in ${cityName}`,
                ];

                let registeredForCity = 0;
                let discoveredForCity = 0;

                for (const query of queries) {
                    const response = await mapsClient.textSearch({
                        params: {
                            query,
                            key: GOOGLE_MAPS_API_KEY.value(),
                        },
                    });

                    const results = response.data.results || [];
                    discoveredForCity += results.length;
                    console.log(`📍 Found ${results.length} results for ${cityName} (${query})`);

                    for (const place of results) {
                        try {
                            const details = await mapsClient.placeDetails({
                                params: {
                                    place_id: place.place_id,
                                    fields: ["name", "website", "geometry"],
                                    key: GOOGLE_MAPS_API_KEY.value(),
                                },
                            });

                            const website = details.data?.result?.website;
                            const name = details.data?.result?.name || place.name;
                            const loc = details.data?.result?.geometry?.location;

                            if (website && loc) {
                                const urlDocId = Buffer.from(website)
                                    .toString("base64")
                                    .substring(0, 100);

                                await db.collection("url_registry").doc(urlDocId).set(
                                    {
                                        url_hash: website,
                                        venue_name: name,
                                        city: cityName,
                                        latitude: loc.lat,
                                        longitude: loc.lng,
                                        last_discovered: admin.firestore.FieldValue.serverTimestamp(),
                                    },
                                    { merge: true }
                                );

                                registeredForCity++;
                                totalRegistered++;
                                console.log(`✅ Registered: ${name} (${website})`);
                            }
                        } catch (placeError) {
                            const detailMessage =
                                placeError?.response?.data?.error_message ||
                                placeError?.message ||
                                placeError;
                            console.error(
                                `⚠️ Error getting details for place: ${place.name} (${place.place_id})`,
                                detailMessage
                            );
                        }
                    }
                }

                totalDiscovered += discoveredForCity;

                // Mark city as complete
                await cityRef.set(
                    {
                        status: "complete",
                        last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                        libraries_found: registeredForCity,
                        queries_run: queries.length,
                    },
                    { merge: true }
                );
            } catch (error) {
                const errorMessage =
                    error?.response?.data?.error_message ||
                    error?.response?.data?.error?.message ||
                    error?.message ||
                    error;

                console.error(`❌ Error searching ${cityName}:`, errorMessage);
                await cityRef.set(
                    {
                        status: "error",
                        last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                        error_message: String(errorMessage),
                        error_status: error?.response?.status || null,
                    },
                    { merge: true }
                );
            }
        }

        console.log(`🎉 Discovery Complete: Found ${totalDiscovered} libraries, Registered ${totalRegistered} new URLs`);
    },
);

/**
 * Scheduled Fetcher: Serper.dev Google Events (Weekly)
 * Searches toddler-friendly events by COUNTY to minimize API calls.
 * Runs weekly to stay under 250 queries/month limit (~4 runs × 9 counties = 36 queries).
 */
exports.serperDevFetchAndFilterEvents = onSchedule(
    {
        schedule: "0 0 * * 0", // Every Sunday at midnight UTC (weekly)
        secrets: [SERPER_DEV_API_KEY, GOOGLE_MAPS_API_KEY, GEMINI_API_KEY],
        timeoutSeconds: 180,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🕵️ Starting Serper.dev toddler events fetch (weekly by county)...");

        const citiesSnap = await db.collection("config_cities").get();
        const countiesSet = new Set();

        if (!citiesSnap.empty) {
            citiesSnap.docs.forEach((doc) => {
                const county = doc.data()?.county;
                if (county) countiesSet.add(county);
            });
        }

        const countiesToSearch = countiesSet.size > 0
            ? Array.from(countiesSet)
            : [
                "Alameda County, CA",
                "Contra Costa County, CA",
                "Marin County, CA",
                "San Mateo County, CA",
                "Santa Clara County, CA",
                "San Francisco County, CA",
                "Solano County, CA",
                "Napa County, CA",
            ];

        console.log(`🔎 Will search ${countiesToSearch.length} unique counties`);

        const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
        const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });

        let totalAdded = 0;

        for (let i = 0; i < countiesToSearch.length; i++) {
            const county = countiesToSearch[i];
            try {
                if (i > 0) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }

                const searchQueries = [
                    `toddler storytime ${county}`,
                    `baby activities ${county}`,
                ];

                const allOrganicResults = [];
                const seenUrls = new Set();

                for (const query of searchQueries) {
                    try {
                        console.log(`🔎 Searching: "${query}"...`);
                        const response = await axios.post(
                            "https://google.serper.dev/search",
                            {
                                q: query,
                                gl: "us",
                                hl: "en",
                                num: 20,
                            },
                            {
                                headers: {
                                    "X-API-KEY": SERPER_DEV_API_KEY.value(),
                                    "Content-Type": "application/json",
                                },
                            }
                        );

                        const results = response.data?.organic || [];
                        for (const result of results) {
                            if (!seenUrls.has(result.link)) {
                                seenUrls.add(result.link);
                                allOrganicResults.push(result);
                            }
                        }

                        await new Promise(resolve => setTimeout(resolve, 1500));
                    } catch (queryErr) {
                        console.warn(`   ⚠️ Error with query "${query}":`, queryErr.message);
                    }
                }

                console.log(`   🌐 Found ${allOrganicResults.length} unique results in ${county}`);

                if (allOrganicResults.length === 0) {
                    console.log(`   ⚠️ No results found for ${county}`);
                    continue;
                }

                const limitedResults = allOrganicResults.slice(0, 30);
                if (allOrganicResults.length > 30) {
                    console.log(`   ⚠️ Limiting to 30 results (found ${allOrganicResults.length})`);
                }

                const snippets = limitedResults.map((r, i) => `[${i}] Title: ${r.title}\nURL: ${r.link}\nSnippet: ${r.snippet}`).join("\n\n");
                const now = new Date();
                const currentYear = now.getFullYear();
                const currentMonth = String(now.getMonth() + 1).padStart(2, '0');
                const currentDay = String(now.getDate()).padStart(2, '0');

                const prompt = `Extract upcoming toddler events (ages 0-4) from these search results in ${county}.

Today is ${currentYear}-${currentMonth}-${currentDay}.

Extract for EACH event:
- title: event name (full descriptive title)
- date: YYYY-MM-DD format (assume ${currentYear} if only month/day given; use next occurrence for recurring events)
- location: FULL venue name with city. For online/virtual events, include "Online" or "Virtual"
- description: brief 1-2 sentences
- sourceUrl: the URL from the search result

Include: approximate dates, recurring events (use next occurrence), online/virtual events, events from calendars/library websites
Skip: blog posts, reviews, general info pages (unless they list specific events)

Return ONLY valid JSON array: [{"title": "...", "date": "...", "location": "...", "description": "...", "sourceUrl": "..."}]

${snippets}`;

                console.log(`   🤖 Extracting events from ${limitedResults.length} results...`);
                try {
                    const geminiResponse = await model.generateContent(prompt);
                    const responseText = geminiResponse.response.text() || "[]";

                    let extractedEvents = [];
                    try {
                        let cleanedText = responseText.trim()
                            .replace(/```json\s*/g, '')
                            .replace(/```\s*/g, '');

                        const jsonMatch = cleanedText.match(/\[[\s\S]*\]/);
                        if (jsonMatch) {
                            extractedEvents = JSON.parse(jsonMatch[0]);
                        } else {
                            const objectMatch = cleanedText.match(/\{[\s\S]*\}/);
                            if (objectMatch) {
                                const parsed = JSON.parse(objectMatch[0]);
                                extractedEvents = parsed.events || (Array.isArray(parsed) ? parsed : []);
                            }
                        }

                        if (!Array.isArray(extractedEvents)) {
                            extractedEvents = [];
                        }
                    } catch (jsonErr) {
                        console.warn(`   ⚠️ Failed to parse Gemini JSON:`, jsonErr.message);
                    }

                    console.log(`   ✅ Extracted ${extractedEvents.length} events`);

                    let addedForCounty = 0;
                    let skippedForCounty = 0;
                    for (const event of extractedEvents) {
                        try {
                            const title = String(event.title || "").trim();
                            const location = String(event.location || county).trim();
                            let dateStr = String(event.date || "").trim();

                            if (!title || title.length < 3) {
                                skippedForCounty++;
                                continue;
                            }

                            const locationLower = location.toLowerCase();
                            const isVirtual = locationLower.includes("online") ||
                                locationLower.includes("virtual") ||
                                locationLower.includes("zoom");

                            const invalidLocations = ["tbd", "unknown", "various locations", "multiple locations"];
                            if (invalidLocations.some(inv => locationLower === inv) || location.length < 3) {
                                skippedForCounty++;
                                continue;
                            }

                            let eventDate = null;
                            if (dateStr && !["TBD", "ongoing", "TBA"].includes(dateStr)) {
                                eventDate = new Date(dateStr);

                                if (isNaN(eventDate.getTime())) {
                                    const monthDayYear = dateStr.match(/(\w+)\s+(\d+),?\s+(\d{4})/i);
                                    if (monthDayYear) {
                                        eventDate = new Date(`${monthDayYear[1]} ${monthDayYear[2]}, ${monthDayYear[3]}`);
                                    }

                                    const slashDate = dateStr.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
                                    if (slashDate && isNaN(eventDate?.getTime())) {
                                        const year = slashDate[3].length === 2 ? `20${slashDate[3]}` : slashDate[3];
                                        eventDate = new Date(`${year}-${slashDate[1].padStart(2, '0')}-${slashDate[2].padStart(2, '0')}`);
                                    }

                                    const monthDay = dateStr.match(/(\w+)\s+(\d+)/i);
                                    if (monthDay && isNaN(eventDate?.getTime())) {
                                        const currentYear = new Date().getFullYear();
                                        eventDate = new Date(`${monthDay[1]} ${monthDay[2]}, ${currentYear}`);
                                        if (eventDate.getTime() < Date.now() - 7 * 24 * 60 * 60 * 1000) {
                                            eventDate = new Date(`${monthDay[1]} ${monthDay[2]}, ${currentYear + 1}`);
                                        }
                                    }

                                    if (isNaN(eventDate?.getTime())) {
                                        eventDate = null;
                                    }
                                }
                            }

                            if (!eventDate || eventDate.getTime() < Date.now() - 24 * 60 * 60 * 1000) {
                                skippedForCounty++;
                                continue;
                            }

                            const mapsKey = GOOGLE_MAPS_API_KEY.value();
                            let coords = await getDynamicCoordinates(location, mapsKey);

                            if (!coords.lat || !coords.lng) {
                                await new Promise(resolve => setTimeout(resolve, 500));
                                coords = await getDynamicCoordinates(location, mapsKey);
                            }

                            if (!coords.lat || !coords.lng) {
                                const cityName = location.split(',')[0].trim() || county.split(' County')[0].trim();
                                coords = await getDynamicCoordinates(`${cityName}, California`, mapsKey);
                            }

                            if (isVirtual && (!coords.lat || !coords.lng)) {
                                coords = { lat: 37.7749, lng: -122.4194, address: "Virtual/Online Event" };
                            }

                            if (!coords.lat || !coords.lng) {
                                skippedForCounty++;
                                continue;
                            }

                            const dateKey = `${eventDate.getFullYear()}-${String(eventDate.getMonth() + 1).padStart(2, "0")}-${String(eventDate.getDate()).padStart(2, "0")}`;
                            const uniqueId = generateEventId(title, location, dateKey);
                            const docRef = db.collection("activities").doc(uniqueId);
                            const docSnap = await docRef.get();

                            if (docSnap.exists) {
                                skippedForCounty++;
                                continue;
                            }

                            const startSec = Math.floor(eventDate.getTime() / 1000);
                            const endSec = startSec + 2 * 60 * 60;

                            await docRef.set({
                                title,
                                venue: location,
                                description: event.description || null,
                                startTime: startSec,
                                endTime: endSec,
                                ageRange: "0-4 years",
                                isFree: true,
                                requiresBooking: false,
                                registrationUrl: null,
                                latitude: coords.lat,
                                longitude: coords.lng,
                                isIndoor: !isVirtual,
                                geohash: geofire.geohashForLocation([coords.lat, coords.lng]),
                                sourceUrl: event.sourceUrl || null,
                                createdAt: Math.floor(Date.now() / 1000),
                                expireAt: new Date(endSec * 1000 + 10 * 60 * 1000),
                                source: "serper.dev",
                                isVirtual,
                                locationAccuracy: isVirtual ? "approximate" : "exact",
                            }, { merge: true });

                            totalAdded++;
                            addedForCounty++;
                            console.log(`   ✅ Added: "${title}" (${location}, ${dateKey})`);
                        } catch (eventErr) {
                            console.warn(`   ⚠️ Error processing event:`, eventErr?.message || eventErr);
                            skippedForCounty++;
                        }
                    }
                    console.log(`   📈 ${county}: ${addedForCounty} added, ${skippedForCounty} skipped`);
                } catch (geminiErr) {
                    console.error(`   ❌ Gemini error for ${county}:`, geminiErr?.message || geminiErr);
                }
            } catch (e) {
                console.error(`❌ Error processing ${county}:`, e?.message || e);
            }
        }

        console.log(`🎯 Serper.dev fetch complete. Total events added: ${totalAdded}`);
    },
);


