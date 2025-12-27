const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { Client } = require("@googlemaps/google-maps-services-js");
const TurndownService = require("turndown");
const crypto = require("crypto");
const cheerio = require("cheerio");
const geofire = require("geofire-common");

// Define the secrets for API keys
const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");
const GOOGLE_MAPS_API_KEY = defineSecret("GOOGLE_MAPS_API_KEY");

// Initialize the Google Maps Client
const mapsClient = new Client({});

admin.initializeApp();
const db = admin.firestore();

/**
 * Dynamically fetches coordinates for any venue string using Google Maps.
 * @param {string} venueName - The name of the library or place.
 * @param {string} apiKey - Your Google Maps API Key.
 * @returns {Promise<Object>} { lat, lng, formattedAddress }
 */
async function getDynamicCoordinates(venueName, apiKey) {
    try {
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

            // Verify result is in California area (lat between 32.5-42, lng between -124 and -114)
            const lat = result.geometry.location.lat;
            const lng = result.geometry.location.lng;
            const inCaliforniaArea =
                lat >= 32.5 && lat <= 42 && lng <= -114 && lng >= -124;

            if (!inCaliforniaArea) {
                console.warn(
                    `⚠️ Geocoded address is likely outside California: ${address}`,
                );
            }

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

        for (const registryDoc of registrySnap.docs) {
            const libraryData = registryDoc.data();
            const targetUrl = libraryData.url_hash;
            const venueName = libraryData.venue_name || "Library";

            // Skip URLs that don't look like event pages
            if (
                !targetUrl ||
                (!targetUrl.includes("/events") &&
                    !targetUrl.includes("/calendar") &&
                    !targetUrl.includes("programs"))
            ) {
                console.log(
                    `⏭️ Skipping non-event URL: ${venueName} (${targetUrl})`,
                );
                continue;
            }

            totalProcessed++;
            console.log(
                `\n🔄 Processing: ${venueName}\n   URL: ${targetUrl.substring(0, 60)}...`,
            );

            try {
                // 1. Fetch rendered content via Jina Reader
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

                        const normalized = {
                            title: String(act.title || "").trim(),
                            venue: String(act.venue || "").trim(),
                            description: act.description ?? null,
                            startTime: Math.floor(
                                new Date(act.isoDate).getTime() / 1000,
                            ),
                            endTime: act.endTime
                                ? Math.floor(new Date(act.endTime).getTime() / 1000)
                                : null,
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
                            geohash:
                                coordinates.lat && coordinates.lng
                                    ? geofire.geohashForLocation([
                                        coordinates.lat,
                                        coordinates.lng,
                                    ])
                                    : null,
                            sourceUrl: targetUrl,
                            createdAt: Math.floor(Date.now() / 1000),
                            expireAt: new Date(
                                (act.endTime
                                    ? new Date(act.endTime).getTime()
                                    : new Date(act.isoDate).getTime()) +
                                24 * 60 * 60 * 1000,
                            ),
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
    }
);
