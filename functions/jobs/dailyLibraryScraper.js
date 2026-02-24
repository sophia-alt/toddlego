const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("firebase-admin");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const axios = require("axios");
const geofire = require("geofire-common");
const {
    db,
    getDynamicCoordinates,
    generateEventId,
    normalizeAgeRange,
    isPastIsoDate,
    generateContentHash,
    cleanContentForHashing,
    validateCoordinates,
} = require("../utils/helpers");
const { GEMINI_API_KEY, GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

/** Delay (ms) between library requests to avoid overwhelming Jina. */
const DELAY_BETWEEN_LIBRARIES_MS = 1000;

/** Firestore batch size for existence checks (max 30 for 'in' queries). */
const BATCH_GET_SIZE = 30;

/**
 * Daily Library Scraper
 * Scrapes library websites from url_registry for toddler events
 * Runs every 24 hours. Timeout 1800s (Firebase max for scheduled); ensure Scheduler attempt-deadline >= 1800s.
 */
exports.dailyLibraryScraper = onSchedule(
    {
        schedule: "every 24 hours",
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 1800,
        memory: "512MiB",
    },
    async (event) => {
        const runStartMs = Date.now();
        console.log(`[dailyLibraryScraper] START at ${new Date().toISOString()}`);

        const fetchStartMs = Date.now();
        const registrySnap = await db.collection("url_registry").get();
        console.log(`[dailyLibraryScraper] url_registry get took ${Date.now() - fetchStartMs}ms, size=${registrySnap.size}`);

        if (registrySnap.empty) {
            console.log("ℹ️ No libraries in registry. Run discovery function first.");
            return;
        }

        let totalProcessed = 0;
        let totalEventsAdded = 0;

        const maxLibrariesPerRun = 50;
        const librariesToProcess = registrySnap.docs.slice(0, maxLibrariesPerRun);

        if (registrySnap.size > maxLibrariesPerRun) {
            console.log(`⚠️ Limiting to ${maxLibrariesPerRun} libraries per run (${registrySnap.size} total).`);
        }

        for (let libIndex = 0; libIndex < librariesToProcess.length; libIndex++) {
            const registryDoc = librariesToProcess[libIndex];
            const libraryData = registryDoc.data();
            const targetUrl = libraryData.url_hash;
            const venueName = libraryData.venue_name || "Library";
            const libStartMs = Date.now();

            if (!targetUrl) {
                console.log(`⏭️ Skipping empty URL for ${venueName}`);
                continue;
            }

            totalProcessed++;
            console.log(`\n🔄 [${totalProcessed}/${librariesToProcess.length}] ${venueName} (${targetUrl.substring(0, 50)}...)`);

            try {
                if (totalProcessed > 1) {
                    await new Promise((r) => setTimeout(r, DELAY_BETWEEN_LIBRARIES_MS));
                }

                const readerUrl = `https://r.jina.ai/${targetUrl}`;
                let markdown;
                try {
                    const jinaStartMs = Date.now();
                    const fetchResponse = await axios.get(readerUrl, {
                        timeout: 30000,
                        headers: { "Accept": "text/markdown" },
                    });
                    markdown = fetchResponse.data;
                    console.log(`[dailyLibraryScraper] Jina fetch took ${Date.now() - jinaStartMs}ms`);
                } catch (fetchError) {
                    console.warn(
                        `⚠️ Failed to fetch ${venueName}: ${fetchError.response?.statusText || fetchError.message}`,
                    );
                    continue;
                }

                const contentToAnalyze = markdown.substring(0, 80000);
                const cleanedContent = cleanContentForHashing(contentToAnalyze);
                const currentHash = generateContentHash(cleanedContent);

                const urlDocId = registryDoc.id;
                const cacheRef = db.collection("url_registry").doc(urlDocId);
                const cacheDoc = await cacheRef.get();

                if (cacheDoc.exists && cacheDoc.data().content_hash === currentHash) {
                    const lastParsed = new Date(
                        (cacheDoc.data().last_parsed || 0) * 1000,
                    ).toLocaleString();
                    console.log(`✅ Cache Hit! Skipping Gemini. (Last parsed: ${lastParsed}) [library took ${Date.now() - libStartMs}ms]`);
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
        - INCLUDE events for: Babies (0-18m), Toddlers (18-36m), Preschoolers (3-5y), Kids (when 0-5 appropriate).
        - INCLUSION KEYWORDS: Storytime, Preschool Storytime, Toddler Storytime, Bouncing Babies, Play & Learn, Music & Movement, Baby Bounce, Stay & Play, Tiny Tots, Musical Storytime.
        - INCLUDE "Family" or "Everyone" / "All ages" events that are clearly child-friendly (e.g. storytime, family storytime).
        - EXCLUDE: Teens-only, Adults-only, "School-age", "Grades K-5", "Grades K-8" (unless also for younger), Tweens.

        ### 2. DATE & TIME PROCESSING (CRITICAL)
        - Source text may use relative dates (e.g., "Tomorrow", "Next Wednesday") or specific dates (e.g., "Dec 25").
        - Use the current reference year (${new Date().getFullYear()}) unless the text says otherwise.
        - Convert all dates to valid ISO 8601 format (YYYY-MM-DDTHH:mm:ss).
        - If multiple sessions exist for one event, create a separate entry for each date/time.
        - Accept recurring events and use the NEXT occurrence date.

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
                let extractedEvents = [];
                try {
                    const responseText = result.response.text();
                    if (!responseText) {
                        console.warn("⚠️ Empty response from Gemini");
                    } else {
                        const aiResponse = JSON.parse(responseText);
                        extractedEvents = aiResponse.events || [];
                    }
                } catch (parseError) {
                    console.error(`❌ Error parsing Gemini JSON response:`, parseError.message);
                    console.error(`Response text (first 500 chars):`, result.response.text()?.substring(0, 500));
                    // Continue with empty events array
                }

                const geminiMs = Date.now() - libStartMs;
                console.log(`[dailyLibraryScraper] Gemini found ${extractedEvents.length} events (Gemini phase ~${geminiMs}ms)`);

                if (extractedEvents.length === 0) {
                    console.log(`ℹ️ No toddler events found for ${venueName}`);
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

                // Build list of valid candidates with their doc refs
                const candidates = [];
                for (const act of extractedEvents) {
                    if (!act || !act.title || !act.venue || !act.isoDate) continue;
                    if (Number.isNaN(Date.parse(act.isoDate))) continue;
                    if (isPastIsoDate(act.isoDate, 14)) continue;
                    const eventDate = String(act.isoDate).split("T")[0];
                    const uniqueId = generateEventId(String(act.title), String(act.venue), eventDate);
                    candidates.push({ act, uniqueId, docRef: db.collection("activities").doc(uniqueId) });
                }

                // Batch existence check (avoids N sequential get() calls)
                const existingIds = new Set();
                for (let i = 0; i < candidates.length; i += BATCH_GET_SIZE) {
                    const chunk = candidates.slice(i, i + BATCH_GET_SIZE);
                    const snaps = await Promise.all(chunk.map((c) => c.docRef.get()));
                    chunk.forEach((c, j) => {
                        if (snaps[j].exists) existingIds.add(c.uniqueId);
                    });
                }

                const batch = db.batch();
                let newEventsCount = 0;

                for (const { act, uniqueId, docRef } of candidates) {
                    if (existingIds.has(uniqueId)) continue;

                    // Only add if new
                    {
                        const mapsKey = GOOGLE_MAPS_API_KEY.value();

                        // Use pre-discovered coordinates if available, else geocode
                        const coordinates =
                            libraryData.latitude && libraryData.longitude
                                ? {
                                    lat: libraryData.latitude,
                                    lng: libraryData.longitude,
                                    address: libraryData.venue_name,
                                }
                                : await getDynamicCoordinates(act.venue, mapsKey);

                        // Validate coordinates before proceeding
                        if (!coordinates || !coordinates.lat || !coordinates.lng || !validateCoordinates(coordinates.lat, coordinates.lng)) {
                            console.warn(`⚠️ Could not geocode or invalid coordinates: ${act.venue}`);
                            continue;
                        }

                        // Generate geohash with error handling
                        let geohash;
                        try {
                            geohash = geofire.geohashForLocation([coordinates.lat, coordinates.lng]);
                        } catch (geohashErr) {
                            console.warn(`⚠️ Error generating geohash for ${act.venue}:`, geohashErr.message);
                            continue;
                        }

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
                            position: {
                                geohash: geohash,
                                geopoint: new admin.firestore.GeoPoint(
                                    coordinates.lat,
                                    coordinates.lng,
                                ),
                            },
                            sourceUrl: targetUrl,
                            createdAt: Math.floor(Date.now() / 1000),
                            // Expire shortly after the event ends to avoid showing stale entries.
                            expireAt: new Date(
                                (act.endTime
                                    ? new Date(act.endTime).getTime()
                                    : new Date(act.isoDate).getTime() + 2 * 60 * 60 * 1000) +
                                5 * 60 * 1000 // small buffer
                            ),
                            // New structured schema fields (dual-write for compatibility)
                            type: "one_time",
                            category: "Library Program",
                            location: {
                                name: String(act.venue || venueName || "").trim(),
                                geohash: geohash,
                                geopoint: new admin.firestore.GeoPoint(
                                    coordinates.lat,
                                    coordinates.lng,
                                ),
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
                console.log(`[dailyLibraryScraper] Library ${venueName} completed in ${Date.now() - libStartMs}ms (added ${newEventsCount} events)`);
            } catch (error) {
                console.error(`❌ Error processing ${venueName}:`, error.message);
            }
        }

        const runDurationMs = Date.now() - runStartMs;
        console.log(`[dailyLibraryScraper] END at ${new Date().toISOString()}, duration=${runDurationMs}ms, processed=${totalProcessed}, eventsAdded=${totalEventsAdded}`);
        console.log(`\n🎉 Daily Scraper Complete:\n   Processed: ${totalProcessed} libraries\n   Events Added: ${totalEventsAdded}`);
    },
);
