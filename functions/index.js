const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { Client } = require("@googlemaps/google-maps-services-js");
const TurndownService = require("turndown");
const crypto = require("crypto");

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
        const response = await mapsClient.geocode({
            params: {
                address: `${venueName}, Austin, TX`,
                key: apiKey
            }
        });

        if (response.data.results.length > 0) {
            const result = response.data.results[0];
            console.log(`📍 Geocoded ${venueName}: ${result.formatted_address}`);
            return {
                lat: result.geometry.location.lat,
                lng: result.geometry.location.lng,
                address: result.formatted_address
            };
        }
    } catch (error) {
        console.error(`[Geocoding Error] Could not find: ${venueName}`, error.message);
    }

    return { lat: null, lng: null, address: null };
}

/**
 * Helper to create a unique, URL-safe ID for each event.
 * Using Title + Venue + Date ensures recurring events are saved separately.
 */
const generateEventId = (title, venue, date) => {
    const rawStr = `${title}-${venue}-${date}`.toLowerCase().replace(/\s+/g, '-');
    return Buffer.from(rawStr).toString('base64').substring(0, 50);
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
    return crypto.createHash('sha256').update(content, 'utf8').digest('hex');
};

exports.dailyLibraryScraper = onSchedule({
    schedule: "every 24 hours",
    secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
    timeoutSeconds: 300, // Increased for AI processing
    memory: "512MiB"     // Increased for larger text payloads
}, async (event) => {
    console.log("🚀 Starting Toddlego Library Scraper...");

    // BiblioCommons is an SPA; using r.jina.ai converts the rendered JS into clean Markdown
    const targetUrl = "https://aclibrary.bibliocommons.com/v2/events";
    const readerUrl = `https://r.jina.ai/${targetUrl}`;

    try {
        // 1. Fetch rendered content via Jina Reader
        const fetchResponse = await fetch(readerUrl);
        if (!fetchResponse.ok) throw new Error(`Failed to fetch from Jina: ${fetchResponse.statusText}`);

        const markdown = await fetchResponse.text();
        console.log("🌐 Content fetched and converted to Markdown.");

        // 2. Check cache to avoid unnecessary Gemini calls
        const contentToAnalyze = markdown.substring(0, 40000);
        const currentHash = generateContentHash(contentToAnalyze);

        // Create safe Firestore doc ID from URL
        const urlDocId = Buffer.from(targetUrl).toString('base64').substring(0, 100);
        const cacheRef = db.collection('url_registry').doc(urlDocId);
        const cacheDoc = await cacheRef.get();

        // Check if content unchanged
        if (cacheDoc.exists && cacheDoc.data().content_hash === currentHash) {
            const lastParsed = new Date((cacheDoc.data().last_parsed || 0) * 1000).toLocaleString();
            console.log("✅ Cache Hit! Content unchanged since last scrape. Skipping Gemini API call.");
            console.log(`ℹ️ Last parsed: ${lastParsed}, Found ${cacheDoc.data().event_count || 0} events previously.`);
            console.log("💡 No action needed - library page content has not changed.");
            return;
        }

        console.log("🔄 Cache Miss. Content changed or first run. Calling Gemini...");

        // 3. Initialize Gemini 2.0 Flash
        const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
        const model = genAI.getGenerativeModel({
            model: "gemini-2.0-flash",
            generationConfig: {
                responseMimeType: "application/json",
            }
        });

        // 4. AI Analysis with strict instructions
        const prompt = `
            You are a specialized data extractor for "Toddlego," an app for parents of children aged 0-4.
            Extract library events from the provided markdown text.

            STRICT FILTERING RULES:
            - ONLY include events for: Babies (0-18m), Toddlers (18m-3y), or Preschoolers (3-5y).
            - Keywords to look for: Storytime, Play & Learn, Music & Movement, Baby Bounce, Stay & Play.
            - EXCLUDE: Teen events, Adult computer classes, and general "School-age" crafts.
            - "Family" events are ONLY included if the description mentions activities for toddlers.

            OUTPUT FORMAT:
            Return a JSON object with a key "events" containing an array of objects:
            {
              "events": [
                {
                  "title": "Clear event name",
                  "venue": "Specific Library Branch name",
                  "description": "A warm, 2-sentence summary for a tired parent. Mention if there are bubbles, songs, or toys.",
                  "isoDate": "YYYY-MM-DDTHH:mm:ss",
                  "ageRange": "e.g., 0-24 months",
                  "isRegistrationRequired": boolean,
                  "registrationUrl": "URL if applicable, else null"
                }
              ]
            }

            CONTENT TO ANALYZE:
            ${contentToAnalyze}
        `;

        const result = await model.generateContent(prompt);
        const aiResponse = JSON.parse(result.response.text());
        const extractedEvents = aiResponse.events || [];

        console.log(`🤖 Gemini found ${extractedEvents.length} relevant toddler events.`);

        if (extractedEvents.length === 0) {
            console.log("ℹ️ No toddler events found in this scrape.");
            // Update cache even with no events to prevent repeated calls
            await cacheRef.set({
                url_hash: targetUrl,
                last_parsed: Math.floor(Date.now() / 1000),
                content_hash: currentHash,
                parsed_json: JSON.stringify([]),
                event_count: 0
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
                console.warn("⚠️ Skipping event with invalid isoDate:", act.isoDate, act);
                continue;
            }
            const eventDate = String(act.isoDate).split('T')[0];
            const uniqueId = generateEventId(String(act.title), String(act.venue), eventDate);

            const docRef = db.collection("activities").doc(uniqueId);
            const docSnap = await docRef.get();

            // Only add if it doesn't exist
            if (!docSnap.exists) {
                // Skip past events
                if (isPastIsoDate(act.isoDate)) {
                    console.log(`⏭️ Skipping past event: ${act.title} (${act.isoDate})`);
                    continue;
                }
                // Fetch coordinates for the venue (fallback to env var if emulator)
                const mapsKey = (GOOGLE_MAPS_API_KEY && typeof GOOGLE_MAPS_API_KEY.value === 'function')
                    ? GOOGLE_MAPS_API_KEY.value()
                    : process.env.GOOGLE_MAPS_API_KEY;
                const coordinates = await getDynamicCoordinates(act.venue, mapsKey);

                // Normalize fields to match client expectations
                const normalized = {
                    title: String(act.title || '').trim(),
                    venue: String(act.venue || '').trim(),
                    description: act.description ?? null,
                    startTime: Math.floor(new Date(act.isoDate).getTime() / 1000),
                    endTime: act.endTime ? Math.floor(new Date(act.endTime).getTime() / 1000) : null,
                    ageRange: normalizeAgeRange(act.ageRange) ?? 'All',
                    isFree: true, // Library events are usually free
                    requiresBooking: !!act.isRegistrationRequired,
                    registrationUrl: (act.registrationUrl && /^https?:\/\//.test(act.registrationUrl)) ? act.registrationUrl : null,
                    latitude: coordinates.lat,
                    longitude: coordinates.lng,
                    sourceUrl: targetUrl,
                    createdAt: Math.floor(Date.now() / 1000)
                };

                batch.set(docRef, normalized);
                newEventsCount++;
            }
        }

        if (newEventsCount > 0) {
            await batch.commit();
            console.log(`✅ Successfully added ${newEventsCount} new events to Firestore.`);

            // Update cache with successful parse
            await cacheRef.set({
                url_hash: targetUrl,
                last_parsed: Math.floor(Date.now() / 1000),
                content_hash: currentHash,
                parsed_json: JSON.stringify(extractedEvents),
                event_count: newEventsCount
            });
        } else {
            console.log("ℹ️ No new events found (all were duplicates).");

            // Update cache even with duplicates to prevent repeated parsing
            await cacheRef.set({
                url_hash: targetUrl,
                last_parsed: Math.floor(Date.now() / 1000),
                content_hash: currentHash,
                parsed_json: JSON.stringify(extractedEvents),
                event_count: 0
            });
        }

    } catch (error) {
        console.error("❌ Scraper Task Failed:", error);
        throw error; // Ensure Cloud Functions logs the failure
    }
});