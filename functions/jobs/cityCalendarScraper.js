const { onSchedule } = require("firebase-functions/v2/scheduler");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const axios = require("axios");
const {
    db,
    GEMINI_MODEL,
    GEMINI_MAX_OUTPUT_TOKENS,
    createRunBudget,
    getReusableParsedEvents,
    generateEventId,
    activityExists,
    normalizeAgeRange,
    isPastIsoDate,
    generateContentHash,
    cleanContentForHashing,
} = require("../utils/helpers");
const { resolveVenue, venueLocationFields, knownCoordsFor } = require("../utils/venues");
const { linkSeries, seriesOccurrence } = require("../utils/series");
const { PARSE_VERSION, scheduleInstructions, expandToSessions } = require("../utils/schedule");
const { GEMINI_API_KEY, GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

/** Calendars scraped per run; one Gemini call each at most. */
const MAX_CALENDARS_PER_RUN = 25;

/** 17+ pages at up to ~60s each (Jina + Gemini) do not fit in the old 300s timeout. */
const TIMEOUT_SECONDS = 1800;

/**
 * City Calendar Scraper (Weekly)
 * Scrapes city recreation department websites for toddler events
 * Uses existing Jina Reader + Gemini infrastructure (FREE)
 * Runs weekly on Tuesdays at 2 AM UTC (staggered from other weekly jobs)
 */
exports.cityCalendarScraper = onSchedule(
    {
        schedule: "0 2 * * 2", // Every Tuesday at 2 AM UTC
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🏛️ Starting City Calendar Scraper...");
        const budget = createRunBudget({
            geminiCalls: MAX_CALENDARS_PER_RUN,
            geocodeCalls: 80,
            timeoutSeconds: TIMEOUT_SECONDS,
        });

        // Known Bay Area city recreation department event calendar URLs
        // These can be manually curated or discovered via Serper.dev
        // Store in Firestore collection "city_calendars" with structure:
        // { url, city_name, venue_name, latitude, longitude }
        let cityCalendars = [];

        try {
            const calendarsSnap = await db.collection("city_calendars").get();
            cityCalendars = calendarsSnap.docs.map(doc => ({
                id: doc.id,
                ...doc.data(),
            }));
            console.log(`📋 Found ${cityCalendars.length} city calendars in Firestore`);
        } catch (e) {
            console.log("ℹ️ No city_calendars collection found.");
        }

        // Hardcoded known city calendar URLs (add more in Firestore city_calendars)
        const knownCityCalendars = [
            // City recreation departments
            { url: "https://sfrecpark.org/events", city_name: "San Francisco", venue_name: "SF Rec & Park" },
            { url: "https://www.oaklandca.gov/events", city_name: "Oakland", venue_name: "Oakland Parks & Rec" },
            { url: "https://www.haywardrec.org/Calendar.aspx", city_name: "Hayward", venue_name: "Hayward Area Recreation and Park District" },
            // Hayward Public Library (librarycalendar.com - Babies & Toddlers, Preschoolers, Kids, Storytime)
            { url: "https://hayward.librarycalendar.com/events/week", city_name: "Hayward", venue_name: "Hayward Public Library" },
            { url: "https://hayward.librarycalendar.com/events", city_name: "Hayward", venue_name: "Hayward Public Library" },
            // Bay Area libraries (events pages for Jina + Gemini scraping).
            // Libraries on BiblioCommons (Alameda County, San Mateo County, Marin, Contra Costa,
            // Santa Clara County, San Jose, ...) come from bibliocommonsRssSync instead.
            { url: "https://www.berkeleypubliclibrary.org/events/calendar/month", city_name: "Berkeley", venue_name: "Berkeley Public Library" },
            { url: "https://library.livermoreca.gov/events-services/event-calendars/children", city_name: "Livermore", venue_name: "Livermore Public Library" },
        ];

        // Combine Firestore and hardcoded calendars. city_calendars also holds the parse-cache
        // docs written for hardcoded calendars (no url field), so keep only real entries, once per URL.
        const seenUrls = new Set();
        const allCalendars = [...cityCalendars, ...knownCityCalendars]
            .filter((c) => c.url && !seenUrls.has(c.url) && seenUrls.add(c.url))
            .slice(0, MAX_CALENDARS_PER_RUN);

        if (allCalendars.length === 0) {
            console.log("ℹ️ No city calendars configured. Add URLs to city_calendars collection.");
            console.log("   You can discover city calendar URLs using Serper.dev searches.");
            return;
        }

        let totalProcessed = 0;
        let totalEventsAdded = 0;
        const venueMemo = new Map();

        for (const calendarConfig of allCalendars) {
            if (budget.outOfTime()) {
                console.warn(`⏱️ Stopping early to stay within the ${TIMEOUT_SECONDS}s timeout`);
                break;
            }
            const targetUrl = calendarConfig.url;
            const cityName = calendarConfig.city_name || "Bay Area";
            const venueName = calendarConfig.venue_name || `${cityName} Recreation Department`;

            if (!targetUrl) {
                console.log(`⏭️ Skipping empty URL for ${venueName}`);
                continue;
            }

            totalProcessed++;
            console.log(`\n🔄 Processing: ${venueName}\n   URL: ${targetUrl.substring(0, 60)}...`);

            try {
                // Add rate limiting
                if (totalProcessed > 1) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }

                // Fetch via Jina Reader (same as library scraper)
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
                    console.warn(`⚠️ Failed to fetch ${venueName}: ${fetchError.response?.statusText || fetchError.message}`);
                    continue;
                }

                // Check cache (same as library scraper). Use larger window for BiblioCommons (many events per page).
                const contentToAnalyze = markdown.substring(0, 80000);
                const cleanedContent = cleanContentForHashing(contentToAnalyze);
                const currentHash = generateContentHash(cleanedContent);

                const cacheDocId = calendarConfig.id || Buffer.from(targetUrl).toString("base64").replace(/[\/+=]/g, "").substring(0, 100);
                const cacheRef = db.collection("city_calendars").doc(cacheDocId);
                const cacheDoc = await cacheRef.get();

                // Unchanged page parsed recently: rebuild its activities from the cached parse.
                // Activities deleted by TTL are re-created below; existing ones are skipped.
                let extractedEvents = getReusableParsedEvents(cacheDoc.data(), currentHash, PARSE_VERSION);
                const fromGemini = extractedEvents === null;

                if (!fromGemini) {
                    const lastParsed = new Date((cacheDoc.data().last_parsed || 0) * 1000).toLocaleString();
                    console.log(`✅ Cache Hit! Reusing ${extractedEvents.length} parsed programs, skipping Gemini. (Last parsed: ${lastParsed})`);
                } else {
                    if (budget.geminiCallsLeft <= 0) {
                        console.warn(`💰 Gemini call budget used up - skipping ${venueName}`);
                        continue;
                    }
                    budget.geminiCallsLeft--;

                    console.log("🔄 Cache Miss - Calling Gemini for analysis...");

                    // Initialize Gemini
                    const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
                    const model = genAI.getGenerativeModel({
                        model: GEMINI_MODEL,
                        generationConfig: {
                            responseMimeType: "application/json",
                            maxOutputTokens: GEMINI_MAX_OUTPUT_TOKENS,
                        },
                    });

                    // AI Analysis prompt (include library audience labels: Babies & Toddlers, Kids, Preschoolers)
                    const prompt = `
            You are extracting toddler/kids events (ages 0-5) from a library or recreation event calendar.
            
            INCLUDE events tagged or described for:
            - Babies & Toddlers, Babies, Toddlers, Preschoolers, Kids (when age-appropriate for 0-5)
            - Storytime, Preschool Storytime, Toddler Storytime, Bouncing Babies, Musical Storytime, Baby Bounce, Stay & Play, Tiny Tots
            
            INCLUDE "Everyone" or "All ages" events that are clearly child-friendly (e.g. storytime, family).
            
            EXCLUDE: Teens, Adults-only, School-age (K-5), Grades K-8 (unless also for younger kids), Tweens
            
            For "venue" use the full branch/location name (e.g. "Alameda County Library - Fremont").
            ${scheduleInstructions()}
            Return JSON with this structure (one entry per program, not per session):
            {
              "events": [
                {
                  "title": "Event name",
                  "venue": "Full venue name with city",
                  "description": "Brief description",
                  "startTime": "HH:mm",
                  "endTime": "HH:mm or null",
                  "dates": ["YYYY-MM-DD"],
                  "recurrence": null or { "byDay": ["WE"], "intervalWeeks": 1, "startDate": null, "endDate": null },
                  "exceptDates": [],
                  "ageRange": "0-2 years or similar",
                  "isRegistrationRequired": boolean,
                  "registrationUrl": "URL if found",
                  "isIndoor": true
                }
              ]
            }
            
            Content to analyze:
            ${contentToAnalyze}
            `;

                    const result = await model.generateContent(prompt);
                    extractedEvents = [];
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

                    console.log(`🤖 Gemini found ${extractedEvents.length} relevant programs`);
                }

                if (extractedEvents.length === 0) {
                    // Update cache even if no events found
                    if (fromGemini) {
                        await cacheRef.set(
                            {
                                content_hash: currentHash,
                                last_parsed: Math.floor(Date.now() / 1000),
                                event_count: 0,
                                parsed_json: JSON.stringify([]),
                                parse_version: PARSE_VERSION,
                            },
                            { merge: true },
                        );
                    }
                    continue;
                }

                // Build candidates and batch existence check (avoids N sequential get() calls)
                const BATCH_GET_SIZE = 30;
                const candidates = [];
                // Programs -> dated sessions (listed dates plus the next weeks of a weekly rule)
                const sessions = extractedEvents.flatMap((program) => expandToSessions(program));
                console.log(`🗓️ ${extractedEvents.length} programs -> ${sessions.length} sessions`);
                for (const act of sessions) {
                    if (!act || !act.title || !act.venue || !act.isoDate) continue;
                    if (Number.isNaN(Date.parse(act.isoDate))) continue;
                    if (isPastIsoDate(act.isoDate, 14)) continue;
                    const isoDate = String(act.isoDate);
                    const dateKey = isoDate.split("T")[0];
                    // "YYYY-MM-DDTHH:mm" so same-day sessions get distinct IDs
                    const uniqueId = generateEventId(String(act.title), String(act.venue), isoDate.slice(0, 16));
                    candidates.push({ act, uniqueId, dateKey, docRef: db.collection("activities").doc(uniqueId) });
                }
                const existingIds = new Set();
                for (let i = 0; i < candidates.length; i += BATCH_GET_SIZE) {
                    const chunk = candidates.slice(i, i + BATCH_GET_SIZE);
                    const exists = await Promise.all(
                        chunk.map((c) => activityExists(c.docRef, String(c.act.title), String(c.act.venue), c.dateKey)),
                    );
                    chunk.forEach((c, j) => {
                        if (exists[j]) existingIds.add(c.uniqueId);
                    });
                }

                const batch = db.batch();
                let newEventsCount = 0;
                const mapsKey = GOOGLE_MAPS_API_KEY.value();
                const knownCoords =
                    calendarConfig.latitude && calendarConfig.longitude
                        ? { lat: calendarConfig.latitude, lng: calendarConfig.longitude, address: venueName }
                        : null;

                // Every listed session (saved or not) gets its venue, so weekly series see the whole page
                const occurrences = [];
                for (const c of candidates) {
                    const venue = await resolveVenue(String(c.act.venue), {
                        apiKey: mapsKey,
                        budget,
                        memo: venueMemo,
                        knownCoords: knownCoordsFor(c.act.venue, venueName, knownCoords),
                    });
                    if (!venue) {
                        if (!existingIds.has(c.uniqueId)) console.warn(`⚠️ Could not geocode: ${c.act.venue}`);
                        continue;
                    }
                    const startTime = Math.floor(new Date(c.act.isoDate).getTime() / 1000);
                    const endTime = c.act.endTime
                        ? Math.floor(new Date(c.act.endTime).getTime() / 1000)
                        : startTime + 60 * 60;
                    occurrences.push({ ...c, venue, startTime, endTime });
                }
                const seriesLinks = await linkSeries(batch, occurrences.map((o) => seriesOccurrence(o.venue, {
                    venueName: String(o.act.venue).trim(),
                    title: String(o.act.title).trim(),
                    startTime: o.startTime,
                    endTime: o.endTime,
                    sourceUrl: targetUrl,
                    ageRange: normalizeAgeRange(o.act.ageRange) ?? "All",
                })));

                for (const [i, { act, uniqueId, docRef, venue, startTime, endTime }] of occurrences.entries()) {
                    if (existingIds.has(uniqueId)) continue;
                    const series = seriesLinks[i];

                    const normalized = {
                            title: String(act.title || "").trim(),
                            venue: String(act.venue || "").trim(),
                            description: act.description ?? null,
                            startTime,
                            endTime,
                            ageRange: normalizeAgeRange(act.ageRange) ?? "All",
                            isFree: true, // City recreation events are typically free
                            requiresBooking: !!act.isRegistrationRequired,
                            registrationUrl:
                                act.registrationUrl && /^https?:\/\//.test(act.registrationUrl)
                                    ? act.registrationUrl
                                    : null,
                            ...venueLocationFields(venue, String(act.venue || venueName || "").trim()),
                            seriesId: series ? series.seriesId : null,
                            sourceUrl: targetUrl,
                            createdAt: Math.floor(Date.now() / 1000),
                            expireAt: new Date(endTime * 1000 + 5 * 60 * 1000),
                            type: series ? "recurring" : "one_time",
                            category: "City Recreation",
                            timing: {
                                is_all_day: false,
                                start_time: startTime,
                                end_time: endTime,
                                recurrence: series ? series.rrule : null,
                                business_hours: null,
                            },
                            age_range: (() => {
                                const s = normalizeAgeRange(act.ageRange);
                                const m = s && s.match(/(\d+)\s*-\s*(\d+)/);
                                if (m) return [Number(m[1]), Number(m[2])];
                                return null;
                            })(),
                            source: "city_calendar",
                            // "listed": date on the page; "recurrence": generated from its weekly schedule
                            scheduleSource: act.scheduleSource,
                            tags: [],
                        };

                    batch.set(docRef, normalized);
                    newEventsCount++;
                }

                if (newEventsCount > 0 || seriesLinks.some(Boolean)) {
                    await batch.commit();
                    totalEventsAdded += newEventsCount;
                    console.log(`✅ Added ${newEventsCount} new events from ${venueName}`);
                }

                // Update cache (only after a fresh Gemini parse, so reuse still expires after 7 days)
                if (fromGemini) {
                    await cacheRef.set(
                        {
                            content_hash: currentHash,
                            last_parsed: Math.floor(Date.now() / 1000),
                            event_count: newEventsCount,
                            parsed_json: JSON.stringify(extractedEvents),
                            parse_version: PARSE_VERSION,
                        },
                        { merge: true },
                    );
                }
            } catch (error) {
                console.error(`❌ Error processing ${venueName}:`, error.message);
            }
        }

        console.log(
            `\n🎉 City Calendar Scraper Complete:\n   Processed: ${totalProcessed} calendars\n   Events Added: ${totalEventsAdded}\n   Budget: ${budget.summary()}`,
        );
    },
);
