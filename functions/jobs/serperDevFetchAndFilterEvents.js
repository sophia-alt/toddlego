const { onSchedule } = require("firebase-functions/v2/scheduler");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const axios = require("axios");
const geofire = require("geofire-common");
const admin = require("firebase-admin");
const {
    db,
    GEMINI_MODEL,
    GEMINI_MAX_OUTPUT_TOKENS,
    createRunBudget,
    getDynamicCoordinates,
    generateEventId,
    activityExists,
    toDateKey,
    toDateTimeKey,
    parseDate,
    sessionDuration,
    isValidFutureDate,
    validateCoordinates,
} = require("../utils/helpers");
const { resolveVenue, venueLocationFields } = require("../utils/venues");
const { linkSeries, seriesOccurrence } = require("../utils/series");
const { SERPER_DEV_API_KEY, GOOGLE_MAPS_API_KEY, GEMINI_API_KEY } = require("../utils/secrets");

/**
 * Counties searched each run (5 Serper queries + 1 Gemini call per county).
 * config_cities may cover all of California; searching every county would not fit the timeout.
 */
const BAY_AREA_COUNTIES = [
    "Alameda County, CA",
    "Contra Costa County, CA",
    "Marin County, CA",
    "San Mateo County, CA",
    "Santa Clara County, CA",
    "San Francisco County, CA",
    "Solano County, CA",
    "Napa County, CA",
];

/** 8 counties x (5 queries x ~2.5s + Gemini + geocoding) do not fit in the old 180s timeout. */
const TIMEOUT_SECONDS = 540;

/**
 * Enhanced Serper.dev Events Fetcher (Weekly)
 * Searches toddler-friendly events by COUNTY with expanded queries
 * Enhanced with more search terms: parks, community centers, toddler classes
 * Runs weekly on Sundays at midnight UTC
 */
exports.serperDevFetchAndFilterEvents = onSchedule(
    {
        schedule: "0 4 * * 0", // Every Sunday at 04:00 UTC (staggered from discovery)
        secrets: [SERPER_DEV_API_KEY, GOOGLE_MAPS_API_KEY, GEMINI_API_KEY],
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🕵️ Starting Enhanced Serper.dev toddler events fetch (weekly by county)...");
        const budget = createRunBudget({
            geminiCalls: BAY_AREA_COUNTIES.length,
            geocodeCalls: 80,
            timeoutSeconds: TIMEOUT_SECONDS,
        });

        const countiesToSearch = BAY_AREA_COUNTIES;
        console.log(`🔎 Will search ${countiesToSearch.length} unique counties`);

        const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
        const model = genAI.getGenerativeModel({
            model: GEMINI_MODEL,
            generationConfig: { maxOutputTokens: GEMINI_MAX_OUTPUT_TOKENS },
        });

        let totalAdded = 0;
        const venueMemo = new Map();

        for (let i = 0; i < countiesToSearch.length; i++) {
            if (budget.outOfTime()) {
                console.warn(`⏱️ Stopping early to stay within the ${TIMEOUT_SECONDS}s timeout`);
                break;
            }
            const county = countiesToSearch[i];
            try {
                if (i > 0) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }

                // Enhanced search queries: expanded from 2 to 5 queries per county
                const searchQueries = [
                    `toddler storytime ${county}`,
                    `baby activities ${county}`,
                    `toddler classes ${county}`,
                    `parks toddler activities ${county}`,
                    `community center storytime ${county}`,
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

                const prompt = `Extract upcoming toddler/kids events (ages 0-5) from these search results in ${county}. Include storytime, baby/toddler programs, preschool, and family-friendly library/parks events.

Today is ${currentYear}-${currentMonth}-${currentDay}.

Extract for EACH event:
- title: event name (full descriptive title)
- date: YYYY-MM-DD format (assume ${currentYear} if only month/day given; use next occurrence for recurring events)
- time: HH:MM format (24-hour) if available in the source, otherwise null
- location: FULL venue name with city. For online/virtual events, include "Online" or "Virtual"
- description: brief 1-2 sentences
- sourceUrl: the URL from the search result

Include: approximate dates, recurring events (use next occurrence), online/virtual events, events from calendars/library websites
Skip: blog posts, reviews, general info pages (unless they list specific events)

Return ONLY valid JSON array: [{"title": "...", "date": "...", "time": "HH:MM or null", "location": "...", "description": "...", "sourceUrl": "..."}]

${snippets}`;

                if (budget.geminiCallsLeft <= 0) {
                    console.warn(`   💰 Gemini call budget used up - skipping ${county}`);
                    continue;
                }
                budget.geminiCallsLeft--;

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

                            // Validate title and location
                            if (!title || title.length < 3) {
                                skippedForCounty++;
                                continue;
                            }

                            if (!location || location.length < 3) {
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

                            // Parse and validate date (accepts events up to 2 weeks in the past for backfilling)
                            if (!isValidFutureDate(dateStr, 14)) {
                                skippedForCounty++;
                                continue;
                            }

                            const eventDate = parseDate(dateStr);
                            if (!eventDate) {
                                skippedForCounty++;
                                continue;
                            }

                            // Parse time if provided (HH:MM format)
                            let eventDateTime = eventDate;
                            let hasTime = false;
                            const timeStr = String(event.time || "").trim();
                            if (timeStr && /^\d{1,2}:\d{2}$/.test(timeStr)) {
                                const [hours, minutes] = timeStr.split(':').map(Number);
                                if (hours >= 0 && hours < 24 && minutes >= 0 && minutes < 60) {
                                    eventDateTime = new Date(eventDate);
                                    eventDateTime.setHours(hours, minutes, 0, 0);
                                    hasTime = true;
                                }
                            }

                            // Skip known events before paying for geocoding
                            const dateKey = toDateKey(eventDate);
                            const uniqueId = generateEventId(title, location, hasTime ? toDateTimeKey(eventDateTime) : dateKey);
                            const docRef = db.collection("activities").doc(uniqueId);
                            if (await activityExists(docRef, title, location, dateKey)) {
                                skippedForCounty++;
                                continue;
                            }

                            const mapsKey = GOOGLE_MAPS_API_KEY.value();
                            const venue = isVirtual ? null : await resolveVenue(location, { apiKey: mapsKey, budget, memo: venueMemo });

                            // No venue: fall back to the city's coordinates (approximate, no venueId)
                            let coords = venue ? { lat: venue.lat, lng: venue.lng } : { lat: null, lng: null };
                            if (!coords.lat || !coords.lng) {
                                const cityName = location.split(',')[0].trim() || county.split(' County')[0].trim();
                                coords = await getDynamicCoordinates(`${cityName}, California`, mapsKey, budget);
                            }

                            if (isVirtual && (!coords.lat || !coords.lng)) {
                                coords = { lat: 37.7749, lng: -122.4194, address: "Virtual/Online Event" };
                            }

                            // Validate coordinates before proceeding
                            if (!coords || !coords.lat || !coords.lng || !validateCoordinates(coords.lat, coords.lng)) {
                                skippedForCounty++;
                                continue;
                            }

                            const startSec = Math.floor(eventDateTime.getTime() / 1000);
                            // Search snippets give no end time: estimate the length from the title (flagged)
                            const { endTime: endSec, durationMinutes, durationEstimated } = sessionDuration(title, startSec, null);

                            let locationFields;
                            if (venue) {
                                locationFields = venueLocationFields(venue, location);
                            } else {
                                const geohash = geofire.geohashForLocation([coords.lat, coords.lng]);
                                locationFields = {
                                    venueId: null,
                                    latitude: coords.lat,
                                    longitude: coords.lng,
                                    geohash: geohash,
                                    position: {
                                        geohash: geohash,
                                        geopoint: new admin.firestore.GeoPoint(coords.lat, coords.lng),
                                    },
                                    location: {
                                        name: location,
                                        geohash: geohash,
                                        geopoint: new admin.firestore.GeoPoint(coords.lat, coords.lng),
                                    },
                                };
                            }

                            // Search results give only the next session; it joins a series an earlier run found
                            const batch = db.batch();
                            const [series] = hasTime && venue
                                ? await linkSeries(batch, [seriesOccurrence(venue, { venueName: location, title, startTime: startSec, endTime: endSec, durationEstimated, sourceUrl: event.sourceUrl || null, ageRange: "0-4 years" })])
                                : [null];

                            batch.set(docRef, {
                                title,
                                venue: location,
                                description: event.description || null,
                                startTime: startSec,
                                endTime: endSec,
                                durationMinutes,
                                durationEstimated,
                                ageRange: "0-4 years",
                                isFree: true,
                                requiresBooking: false,
                                registrationUrl: null,
                                isIndoor: !isVirtual,
                                ...locationFields,
                                seriesId: series ? series.seriesId : null,
                                sourceUrl: event.sourceUrl || null,
                                createdAt: Math.floor(Date.now() / 1000),
                                expireAt: new Date(endSec * 1000 + 10 * 60 * 1000),
                                source: "serper.dev",
                                isVirtual,
                                locationAccuracy: venue && !isVirtual ? "exact" : "approximate",
                                type: series ? "recurring" : "one_time",
                                category: "Community Event",
                            }, { merge: true });
                            await batch.commit();

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

        console.log(`🎯 Serper.dev fetch complete. Total events added: ${totalAdded}, ${budget.summary()}`);
    },
);
