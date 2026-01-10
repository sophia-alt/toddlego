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
} = require("../utils/helpers");
const { GEMINI_API_KEY, GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

/**
 * Daily Library Scraper
 * Scrapes library websites from url_registry for toddler events
 * Runs every 24 hours
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
                let markdown;
                try {
                    const fetchResponse = await axios.get(readerUrl, {
                        timeout: 30000, // 30 second timeout
                        headers: {
                            "Accept": "text/markdown",
                        },
                    });
                    markdown = fetchResponse.data;
                    console.log(`✅ Fetched ${venueName}`);
                } catch (fetchError) {
                    console.warn(
                        `⚠️ Failed to fetch ${venueName}: ${fetchError.response?.statusText || fetchError.message}`,
                    );
                    continue;
                }

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

                    // Skip events more than 2 weeks in the past (relaxed from original)
                    // Note: isPastIsoDate returns true if date is past threshold, so we skip if true
                    if (isPastIsoDate(act.isoDate, 14)) {
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
