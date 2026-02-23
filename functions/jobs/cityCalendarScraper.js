const { onSchedule } = require("firebase-functions/v2/scheduler");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const axios = require("axios");
const admin = require("firebase-admin");
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

/**
 * City Calendar Scraper (Weekly)
 * Scrapes city recreation department websites for toddler events
 * Uses existing Jina Reader + Gemini infrastructure (FREE)
 * Runs weekly on Mondays at 2 AM UTC
 */
exports.cityCalendarScraper = onSchedule(
    {
        schedule: "0 2 * * 1", // Every Monday at 2 AM UTC
        secrets: [GEMINI_API_KEY, GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 300,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🏛️ Starting City Calendar Scraper...");

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
            // Bay Area libraries (events pages for Jina + Gemini scraping)
            { url: "https://www.berkeleypubliclibrary.org/events/calendar/month", city_name: "Berkeley", venue_name: "Berkeley Public Library" },
            { url: "https://smcl.bibliocommons.com/events", city_name: "San Mateo County", venue_name: "San Mateo County Libraries" },
            { url: "https://marinlibrary.bibliocommons.com/events", city_name: "Marin County", venue_name: "Marin County Free Library" },
            { url: "https://ccclib.bibliocommons.com/events", city_name: "Contra Costa County", venue_name: "Contra Costa County Library" },
            { url: "https://sccld.org/events", city_name: "Santa Clara County", venue_name: "Santa Clara County Library" },
            { url: "https://sjpl.bibliocommons.com/v2/events", city_name: "San Jose", venue_name: "San Jose Public Library" },
            { url: "https://aclibrary.bibliocommons.com/v2/events", city_name: "Alameda County", venue_name: "Alameda County Library" },
            { url: "https://library.livermoreca.gov/events-services/event-calendars/children", city_name: "Livermore", venue_name: "Livermore Public Library" },
        ];

        // Combine Firestore and hardcoded calendars
        const allCalendars = [...cityCalendars, ...knownCityCalendars];

        if (allCalendars.length === 0) {
            console.log("ℹ️ No city calendars configured. Add URLs to city_calendars collection.");
            console.log("   You can discover city calendar URLs using Serper.dev searches.");
            return;
        }

        let totalProcessed = 0;
        let totalEventsAdded = 0;

        for (const calendarConfig of allCalendars) {
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

                // Check cache (same as library scraper)
                const contentToAnalyze = markdown.substring(0, 40000);
                const cleanedContent = cleanContentForHashing(contentToAnalyze);
                const currentHash = generateContentHash(cleanedContent);

                const cacheDocId = calendarConfig.id || Buffer.from(targetUrl).toString("base64").substring(0, 100);
                const cacheRef = db.collection("city_calendars").doc(cacheDocId);
                const cacheDoc = await cacheRef.get();

                if (cacheDoc.exists && cacheDoc.data().content_hash === currentHash) {
                    const lastParsed = new Date((cacheDoc.data().last_parsed || 0) * 1000).toLocaleString();
                    console.log(`✅ Cache Hit! Skipping Gemini. (Last parsed: ${lastParsed})`);
                    continue;
                }

                console.log("🔄 Cache Miss - Calling Gemini for analysis...");

                // Initialize Gemini
                const genAI = new GoogleGenerativeAI(GEMINI_API_KEY.value());
                const model = genAI.getGenerativeModel({
                    model: "gemini-2.0-flash",
                    generationConfig: {
                        responseMimeType: "application/json",
                    },
                });

                // AI Analysis prompt
                const prompt = `
        You are extracting toddler events (ages 0-4) from a city recreation department event calendar.
        
        Extract events that are for:
        - Babies (0-18m), Toddlers (18-36m), Preschoolers (3-5y)
        - Keywords: Storytime, Play & Learn, Music & Movement, Baby Bounce, Stay & Play, Tiny Tots, Toddler Time
        
        Exclude: Teens, Adults, School-age (K-5), Tweens
        
        Return JSON with this structure:
        {
          "events": [
            {
              "title": "Event name",
              "venue": "Full venue name with city",
              "description": "Brief description",
              "isoDate": "YYYY-MM-DDTHH:mm:ss",
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

                console.log(`🤖 Gemini found ${extractedEvents.length} relevant events`);

                if (extractedEvents.length === 0) {
                    // Update cache even if no events found
                    await cacheRef.set(
                        {
                            content_hash: currentHash,
                            last_parsed: Math.floor(Date.now() / 1000),
                            event_count: 0,
                        },
                        { merge: true },
                    );
                    continue;
                }

                // Batch upload events
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

                    // Accept events up to 2 weeks in the past
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

                    if (!docSnap.exists) {
                        const mapsKey = GOOGLE_MAPS_API_KEY.value();

                        // Use pre-configured coordinates if available, else geocode
                        const coordinates =
                            calendarConfig.latitude && calendarConfig.longitude
                                ? {
                                    lat: calendarConfig.latitude,
                                    lng: calendarConfig.longitude,
                                    address: venueName,
                                }
                                : await getDynamicCoordinates(act.venue, mapsKey);

                        if (!coordinates.lat || !coordinates.lng) {
                            console.warn(`⚠️ Could not geocode: ${act.venue}`);
                            continue;
                        }

                        const geohash = geofire.geohashForLocation([coordinates.lat, coordinates.lng]);

                        const normalized = {
                            title: String(act.title || "").trim(),
                            venue: String(act.venue || "").trim(),
                            description: act.description ?? null,
                            startTime: Math.floor(new Date(act.isoDate).getTime() / 1000),
                            endTime: act.endTime
                                ? Math.floor(new Date(act.endTime).getTime() / 1000)
                                : Math.floor(new Date(act.isoDate).getTime() / 1000) + 60 * 60,
                            ageRange: normalizeAgeRange(act.ageRange) ?? "All",
                            isFree: true, // City recreation events are typically free
                            requiresBooking: !!act.isRegistrationRequired,
                            registrationUrl:
                                act.registrationUrl && /^https?:\/\//.test(act.registrationUrl)
                                    ? act.registrationUrl
                                    : null,
                            latitude: coordinates.lat,
                            longitude: coordinates.lng,
                            geohash: geohash,
                            position: {
                                geohash: geohash,
                                geopoint: new admin.firestore.GeoPoint(coordinates.lat, coordinates.lng),
                            },
                            location: {
                                name: String(act.venue || venueName || "").trim(),
                                geohash: geohash,
                                geopoint: new admin.firestore.GeoPoint(coordinates.lat, coordinates.lng),
                            },
                            sourceUrl: targetUrl,
                            createdAt: Math.floor(Date.now() / 1000),
                            expireAt: new Date(
                                (act.endTime
                                    ? new Date(act.endTime).getTime()
                                    : new Date(act.isoDate).getTime() + 2 * 60 * 60 * 1000) +
                                5 * 60 * 1000
                            ),
                            type: "one_time",
                            category: "City Recreation",
                            timing: {
                                is_all_day: false,
                                start_time: Math.floor(new Date(act.isoDate).getTime() / 1000),
                                end_time: act.endTime
                                    ? Math.floor(new Date(act.endTime).getTime() / 1000)
                                    : Math.floor(new Date(act.isoDate).getTime() / 1000) + 60 * 60,
                                recurrence: null,
                                business_hours: null,
                            },
                            age_range: (() => {
                                const s = normalizeAgeRange(act.ageRange);
                                const m = s && s.match(/(\d+)\s*-\s*(\d+)/);
                                if (m) return [Number(m[1]), Number(m[2])];
                                return null;
                            })(),
                            source: "city_calendar",
                            tags: [],
                        };

                        batch.set(docRef, normalized);
                        newEventsCount++;
                    }
                }

                if (newEventsCount > 0) {
                    await batch.commit();
                    totalEventsAdded += newEventsCount;
                    console.log(`✅ Added ${newEventsCount} new events from ${venueName}`);
                }

                // Update cache
                await cacheRef.set(
                    {
                        content_hash: currentHash,
                        last_parsed: Math.floor(Date.now() / 1000),
                        event_count: newEventsCount,
                    },
                    { merge: true },
                );
            } catch (error) {
                console.error(`❌ Error processing ${venueName}:`, error.message);
            }
        }

        console.log(
            `\n🎉 City Calendar Scraper Complete:\n   Processed: ${totalProcessed} calendars\n   Events Added: ${totalEventsAdded}`,
        );
    },
);
