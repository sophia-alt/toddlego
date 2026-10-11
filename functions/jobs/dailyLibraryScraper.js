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
const { linkSeries } = require("../utils/series");
const { GEMINI_API_KEY, GOOGLE_MAPS_API_KEY } = require("../utils/secrets");
const { BAY_AREA_TENANTS, isSupersededUrl } = require("../sources/bibliocommons");

/** Libraries whose events come from bibliocommonsRssSync; no need to scrape them with Gemini. */
const coveredByFeed = (url) => BAY_AREA_TENANTS.some((config) => isSupersededUrl(url, config));

/** Delay (ms) between library requests to avoid overwhelming Jina. */
const DELAY_BETWEEN_LIBRARIES_MS = 1000;

/** Firestore batch size for existence checks (max 30 for 'in' queries). */
const BATCH_GET_SIZE = 30;

/** Libraries scraped per run; one Gemini call each at most (~$0.03 worst case per call). */
const MAX_LIBRARIES_PER_RUN = 40;

const TIMEOUT_SECONDS = 1800;

/**
 * Daily Library Scraper
 * Scrapes library websites from url_registry for toddler events
 * Runs weekly (Wednesdays 03:00 UTC): most registry pages are library home pages that change
 * rarely, so a daily run cost ~30x more for little new data. The export name is kept so the
 * deployed function and its scheduler job are updated in place.
 * Timeout 1800s (Firebase max for scheduled); ensure Scheduler attempt-deadline >= 1800s.
 */
exports.dailyLibraryScraper = onSchedule(
    {
        schedule: "0 3 * * 3", // Every Wednesday at 03:00 UTC (staggered from other weekly jobs)
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        const runStartMs = Date.now();
        const budget = createRunBudget({
            geminiCalls: MAX_LIBRARIES_PER_RUN,
            geocodeCalls: 40,
            timeoutSeconds: TIMEOUT_SECONDS,
        });
        console.log(`[dailyLibraryScraper] START at ${new Date().toISOString()}`);

        const fetchStartMs = Date.now();
        const registrySnap = await db.collection("url_registry").get();
        console.log(`[dailyLibraryScraper] url_registry get took ${Date.now() - fetchStartMs}ms, size=${registrySnap.size}`);

        if (registrySnap.empty) {
            console.log("ℹ️ No libraries in url_registry.");
            return;
        }

        let totalProcessed = 0;
        let totalEventsAdded = 0;
        const venueMemo = new Map();

        // The retired Places discovery job also registered park websites, which rarely list
        // events. Scrape libraries first, then the pages least recently parsed, so a capped run
        // spends Gemini calls well.
        const isLibrary = (doc) => /librar/i.test(doc.data().venue_name || "");
        const scrapeable = registrySnap.docs.filter((doc) => !coveredByFeed(doc.data().url_hash || ""));
        console.log(`[dailyLibraryScraper] ${registrySnap.size - scrapeable.length} entries covered by BiblioCommons RSS, skipped`);
        const sortedDocs = scrapeable.sort((a, b) => {
            if (isLibrary(a) !== isLibrary(b)) return isLibrary(a) ? -1 : 1;
            return (a.data().last_parsed || 0) - (b.data().last_parsed || 0);
        });
        const librariesToProcess = sortedDocs.slice(0, MAX_LIBRARIES_PER_RUN);

        if (registrySnap.size > MAX_LIBRARIES_PER_RUN) {
            console.log(`⚠️ Limiting to ${MAX_LIBRARIES_PER_RUN} libraries per run (${registrySnap.size} total).`);
        }

        for (let libIndex = 0; libIndex < librariesToProcess.length; libIndex++) {
            if (budget.outOfTime()) {
                console.warn(`⏱️ Stopping early to stay within the ${TIMEOUT_SECONDS}s timeout`);
                break;
            }
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

                // Unchanged page parsed recently: rebuild its activities from the cached parse.
                // Activities deleted by TTL are re-created below; existing ones are skipped.
                let extractedEvents = getReusableParsedEvents(cacheDoc.data(), currentHash);
                const fromGemini = extractedEvents === null;

                if (!fromGemini) {
                    const lastParsed = new Date(
                        (cacheDoc.data().last_parsed || 0) * 1000,
                    ).toLocaleString();
                    console.log(`✅ Cache Hit! Reusing ${extractedEvents.length} parsed events, skipping Gemini. (Last parsed: ${lastParsed})`);
                } else {
                    if (budget.geminiCallsLeft <= 0) {
                        console.warn(`💰 Gemini call budget used up - skipping ${venueName}`);
                        continue;
                    }
                    budget.geminiCallsLeft--;

                    console.log("🔄 Cache Miss - Calling Gemini for analysis...");

                    // 3. Initialize Gemini
                    const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
                    const model = genAI.getGenerativeModel({
                        model: GEMINI_MODEL,
                        generationConfig: {
                            responseMimeType: "application/json",
                            maxOutputTokens: GEMINI_MAX_OUTPUT_TOKENS,
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

                    const geminiMs = Date.now() - libStartMs;
                    console.log(`[dailyLibraryScraper] Gemini found ${extractedEvents.length} events (Gemini phase ~${geminiMs}ms)`);
                }

                if (extractedEvents.length === 0) {
                    console.log(`ℹ️ No toddler events found for ${venueName}`);
                    if (fromGemini) {
                        await cacheRef.set(
                            {
                                content_hash: currentHash,
                                last_parsed: Math.floor(Date.now() / 1000),
                                event_count: 0,
                                parsed_json: JSON.stringify([]),
                            },
                            { merge: true },
                        );
                    }
                    continue;
                }

                // Build list of valid candidates with their doc refs
                const candidates = [];
                for (const act of extractedEvents) {
                    if (!act || !act.title || !act.venue || !act.isoDate) continue;
                    if (Number.isNaN(Date.parse(act.isoDate))) continue;
                    if (isPastIsoDate(act.isoDate, 14)) continue;
                    const isoDate = String(act.isoDate);
                    const dateKey = isoDate.split("T")[0];
                    // "YYYY-MM-DDTHH:mm" so same-day sessions get distinct IDs
                    const uniqueId = generateEventId(String(act.title), String(act.venue), isoDate.slice(0, 16));
                    candidates.push({ act, uniqueId, dateKey, docRef: db.collection("activities").doc(uniqueId) });
                }

                // Batch existence check (avoids N sequential get() calls)
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
                // Pre-discovered coordinates belong to this registry entry's own venue: used only for
                // events there, and only when that venue is seen for the first time
                const knownCoords =
                    libraryData.latitude && libraryData.longitude
                        ? { lat: libraryData.latitude, lng: libraryData.longitude, address: libraryData.venue_name }
                        : null;

                // Every listed session (saved or not) gets its venue, so weekly series see the whole page
                const occurrences = [];
                for (const c of candidates) {
                    const venue = await resolveVenue(String(c.act.venue), {
                        apiKey: mapsKey,
                        budget,
                        memo: venueMemo,
                        knownCoords: knownCoordsFor(c.act.venue, libraryData.venue_name, knownCoords),
                    });
                    if (!venue) {
                        if (!existingIds.has(c.uniqueId)) console.warn(`⚠️ Could not geocode or invalid coordinates: ${c.act.venue}`);
                        continue;
                    }
                    const startTime = Math.floor(new Date(c.act.isoDate).getTime() / 1000);
                    // Ensure a reasonable end time if missing (default 1 hour after start)
                    const endTime = c.act.endTime
                        ? Math.floor(new Date(c.act.endTime).getTime() / 1000)
                        : startTime + 60 * 60;
                    occurrences.push({ ...c, venue, startTime, endTime });
                }
                const seriesLinks = await linkSeries(batch, occurrences.map((o) => ({
                    venueId: o.venue.venueId,
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
                        isFree: true,
                        requiresBooking: !!act.isRegistrationRequired,
                        registrationUrl:
                            act.registrationUrl &&
                                /^https?:\/\//.test(act.registrationUrl)
                                ? act.registrationUrl
                                : null,
                        ...venueLocationFields(venue, String(act.venue || venueName || "").trim()),
                        seriesId: series ? series.seriesId : null,
                        sourceUrl: targetUrl,
                        createdAt: Math.floor(Date.now() / 1000),
                        // Expire shortly after the event ends to avoid showing stale entries.
                        expireAt: new Date(endTime * 1000 + 5 * 60 * 1000), // small buffer
                        // New structured schema fields (dual-write for compatibility)
                        type: series ? "recurring" : "one_time",
                        category: "Library Program",
                        timing: {
                            is_all_day: false,
                            start_time: startTime,
                            end_time: endTime,
                            recurrence: series ? series.rrule : null,
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

                if (newEventsCount > 0 || seriesLinks.some(Boolean)) {
                    await batch.commit();
                    totalEventsAdded += newEventsCount;
                    console.log(
                        `✅ Added ${newEventsCount} new events from ${venueName}`,
                    );
                }

                // Update cache (only after a fresh Gemini parse, so reuse still expires after 7 days)
                if (fromGemini) {
                    await cacheRef.set(
                        {
                            content_hash: currentHash,
                            last_parsed: Math.floor(Date.now() / 1000),
                            event_count: newEventsCount,
                            parsed_json: JSON.stringify(extractedEvents),
                        },
                        { merge: true },
                    );
                }
                console.log(`[dailyLibraryScraper] Library ${venueName} completed in ${Date.now() - libStartMs}ms (added ${newEventsCount} events)`);
            } catch (error) {
                console.error(`❌ Error processing ${venueName}:`, error.message);
            }
        }

        const runDurationMs = Date.now() - runStartMs;
        console.log(`[dailyLibraryScraper] END at ${new Date().toISOString()}, duration=${runDurationMs}ms, processed=${totalProcessed}, eventsAdded=${totalEventsAdded}, ${budget.summary()}`);
        console.log(`\n🎉 Daily Scraper Complete:\n   Processed: ${totalProcessed} libraries\n   Events Added: ${totalEventsAdded}`);
    },
);
