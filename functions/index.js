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

exports.dailyLibraryScraper = onSchedule(
    {
        schedule: "every 24 hours",
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 300, // Increased for AI processing
        memory: "512MiB", // Increased for larger text payloads
    },
    async (event) => {
        console.log("🚀 Starting Toddlego Library Scraper...");

        // BiblioCommons events page with full query parameters
        const targetUrl =
            "https://aclibrary.bibliocommons.com/v2/events?_gl=1*ceehy6*_ga*MTc2MDU0NTg1Ni4xNzY2NTMxNzQz*_ga_G99DMMNG39*czE3NjY2MTE5OTEkbzIkZzAkdDE3NjY2MTE5OTEkajYwJGwwJGgw*_ga_DJ3QFJ52TT*czE3NjY2MTE5OTEkbzIkZzAkdDE3NjY2MTE5OTEkajYwJGwwJGgw";
        const readerUrl = `https://r.jina.ai/${targetUrl}`;

        try {
            // 1. Fetch rendered content via Jina Reader
            const fetchResponse = await fetch(readerUrl);
            if (!fetchResponse.ok)
                throw new Error(
                    `Failed to fetch from Jina: ${fetchResponse.statusText}`,
                );

            const markdown = await fetchResponse.text();
            console.log("🌐 Content fetched and converted to Markdown.");

            // 2. Check cache to avoid unnecessary Gemini calls
            const contentToAnalyze = markdown.substring(0, 40000);

            // Clean content before hashing to avoid false cache misses from dynamic elements
            const cleanedContent = cleanContentForHashing(contentToAnalyze);
            const currentHash = generateContentHash(cleanedContent);

            console.log(`🔑 Content hash: ${currentHash.substring(0, 16)}...`);

            // Create safe Firestore doc ID from URL
            const urlDocId = Buffer.from(targetUrl)
                .toString("base64")
                .substring(0, 100);
            const cacheRef = db.collection("url_registry").doc(urlDocId);
            const cacheDoc = await cacheRef.get();

            // Check if content unchanged
            if (cacheDoc.exists && cacheDoc.data().content_hash === currentHash) {
                const lastParsed = new Date(
                    (cacheDoc.data().last_parsed || 0) * 1000,
                ).toLocaleString();
                console.log(
                    "✅ Cache Hit! Content unchanged since last scrape. Skipping Gemini API call.",
                );
                console.log(
                    `ℹ️ Last parsed: ${lastParsed}, Found ${cacheDoc.data().event_count || 0} events previously.`,
                );
                console.log(
                    "💡 No action needed - library page content has not changed.",
                );
                return;
            }

            console.log(
                "🔄 Cache Miss. Content changed or first run. Calling Gemini...",
            );

            // 3. Initialize Gemini 2.0 Flash
            const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
            const model = genAI.getGenerativeModel({
                model: "gemini-2.0-flash",
                generationConfig: {
                    responseMimeType: "application/json",
                },
            });

            // 4. AI Analysis with strict instructions
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
                `🤖 Gemini found ${extractedEvents.length} relevant toddler events.`,
            );

            if (extractedEvents.length === 0) {
                console.log("ℹ️ No toddler events found in this scrape.");
                // Update cache even with no events to prevent repeated calls
                await cacheRef.set({
                    url_hash: targetUrl,
                    last_parsed: Math.floor(Date.now() / 1000),
                    content_hash: currentHash,
                    parsed_json: JSON.stringify([]),
                    event_count: 0,
                });
                return;
            }

            // 5. Batch Upload with Deduplication & Validation
            const batch = db.batch();
            let newEventsCount = 0;

            for (const act of extractedEvents) {
                // Create a unique ID based on Title, Venue, and Date (YYYY-MM-DD)
                if (!act || !act.title || !act.venue || !act.isoDate) {
                    console.warn("⚠️ Skipping invalid event:", act);
                    continue;
                }
                // Ensure isoDate is parseable before generating ID/writes
                const parsed = Date.parse(act.isoDate);
                if (Number.isNaN(parsed)) {
                    console.warn(
                        "⚠️ Skipping event with invalid isoDate:",
                        act.isoDate,
                        act,
                    );
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

                // Only add if it doesn't exist
                if (!docSnap.exists) {
                    // Skip past events
                    if (isPastIsoDate(act.isoDate)) {
                        console.log(
                            `⏭️ Skipping past event: ${act.title} (${act.isoDate})`,
                        );
                        continue;
                    }
                    // Fetch coordinates for the venue (fallback to env var if emulator)
                    const mapsKey =
                        GOOGLE_MAPS_API_KEY &&
                            typeof GOOGLE_MAPS_API_KEY.value === "function"
                            ? GOOGLE_MAPS_API_KEY.value()
                            : process.env.GOOGLE_MAPS_API_KEY;
                    const coordinates = await getDynamicCoordinates(act.venue, mapsKey);

                    // Normalize fields to match client expectations
                    const normalized = {
                        title: String(act.title || "").trim(),
                        venue: String(act.venue || "").trim(),
                        description: act.description ?? null,
                        startTime: Math.floor(new Date(act.isoDate).getTime() / 1000),
                        endTime: act.endTime
                            ? Math.floor(new Date(act.endTime).getTime() / 1000)
                            : null,
                        ageRange: normalizeAgeRange(act.ageRange) ?? "All",
                        isFree: true, // Library events are usually free
                        requiresBooking: !!act.isRegistrationRequired,
                        registrationUrl:
                            act.registrationUrl && /^https?:\/\//.test(act.registrationUrl)
                                ? act.registrationUrl
                                : null,
                        latitude: coordinates.lat,
                        longitude: coordinates.lng,
                        geohash:
                            coordinates.lat && coordinates.lng
                                ? geofire.geohashForLocation([coordinates.lat, coordinates.lng])
                                : null,
                        sourceUrl: targetUrl,
                        createdAt: Math.floor(Date.now() / 1000),
                        // TTL: Auto-delete 24 hours after event ends (or starts if no end time)
                        expireAt: new Date(
                            (act.endTime
                                ? new Date(act.endTime).getTime()
                                : new Date(act.isoDate).getTime()) +
                            24 * 60 * 60 * 1000, // +24 hours in milliseconds
                        ),
                    };

                    batch.set(docRef, normalized);
                    newEventsCount++;
                }
            }

            if (newEventsCount > 0) {
                await batch.commit();
                console.log(
                    `✅ Successfully added ${newEventsCount} new events to Firestore.`,
                );

                // Update cache with successful parse
                await cacheRef.set({
                    url_hash: targetUrl,
                    last_parsed: Math.floor(Date.now() / 1000),
                    content_hash: currentHash,
                    parsed_json: JSON.stringify(extractedEvents),
                    event_count: newEventsCount,
                });
            } else {
                console.log("ℹ️ No new events found (all were duplicates).");

                // Update cache even with duplicates to prevent repeated parsing
                await cacheRef.set({
                    url_hash: targetUrl,
                    last_parsed: Math.floor(Date.now() / 1000),
                    content_hash: currentHash,
                    parsed_json: JSON.stringify(extractedEvents),
                    event_count: 0,
                });
            }
        } catch (error) {
            console.error("❌ Scraper Task Failed:", error);
            throw error; // Ensure Cloud Functions logs the failure
        }
    },
);
