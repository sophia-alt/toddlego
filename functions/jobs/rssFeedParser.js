const { onSchedule } = require("firebase-functions/v2/scheduler");
const axios = require("axios");
const xml2js = require("xml2js");
const admin = require("firebase-admin");
const geofire = require("geofire-common");
const {
    db,
    getDynamicCoordinates,
    generateEventId,
    normalizeAgeRange,
    validateCoordinates,
} = require("../utils/helpers");
const { GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

/**
 * RSS Feed Parser (Daily)
 * Parses RSS feeds from libraries and city event calendars
 * Many libraries and cities publish RSS feeds for events
 * Runs daily to check for new events
 */
exports.rssFeedParser = onSchedule(
    {
        schedule: "every 24 hours",
        secrets: [GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 300,
        memory: "256MiB",
    },
    async (event) => {
        console.log("📡 Starting RSS Feed Parser...");

        // Known RSS feed URLs for Bay Area / California libraries and city calendars
        // Firestore rss_feeds collection can add more; these provide baseline coverage
        const rssFeeds = [
            // Oakland Public Library (EventKeeper) - toddler storytime and kids events
            "https://www.eventkeeper.com/ekfeed/OAKLAND_SRP2011.xml",
            // Santa Cruz Public Library (LibCal) - kids and family programs
            "https://santacruzpl.libcal.com/rss.php?m=audience&iid=4363&cid=8974&audience=309",
            "https://santacruzpl.libcal.com/rss.php?m=audience&iid=4363&cid=8974&audience=310",
            "https://santacruzpl.libcal.com/rss.php?m=month&iid=4363&cid=8974",
            // Woodland Public Library (CivicEngage) - Northern CA
            "https://woodlandpubliclibrary.com/rss.aspx",
        ];

        // Try to get RSS feeds from Firestore collection (if it exists)
        let rssFeedDocs = [];
        try {
            const rssFeedsSnap = await db.collection("rss_feeds").get();
            rssFeedDocs = rssFeedsSnap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
            console.log(`📋 Found ${rssFeedDocs.length} RSS feeds in Firestore`);
        } catch (e) {
            console.log("ℹ️ No rss_feeds collection found. Using hardcoded list or skipping.");
        }

        // Combine hardcoded and Firestore feeds
        const allFeeds = [
            ...rssFeeds.map(url => ({ url, source: "hardcoded" })),
            ...rssFeedDocs.map(doc => ({ url: doc.url, venue_name: doc.venue_name, city: doc.city, latitude: doc.latitude, longitude: doc.longitude, source: "firestore" })),
        ];

        if (allFeeds.length === 0) {
            console.log("ℹ️ No RSS feeds configured. Add RSS feed URLs to rss_feeds collection or hardcode them.");
            return;
        }

        const parser = new xml2js.Parser({
            explicitArray: true,
            mergeAttrs: false,
            explicitRoot: false,
            ignoreAttrs: false,
            trim: true,
            normalize: true,
        });
        let totalAdded = 0;
        let totalProcessed = 0;

        for (const feedConfig of allFeeds) {
            const feedUrl = feedConfig.url;
            if (!feedUrl) continue;

            totalProcessed++;
            console.log(`\n📡 Processing RSS feed: ${feedUrl.substring(0, 60)}...`);

            try {
                // Fetch RSS feed
                const response = await axios.get(feedUrl, {
                    timeout: 10000,
                    headers: {
                        "User-Agent": "Mozilla/5.0 (compatible; ToddlegoBot/1.0)",
                    },
                });

                // Parse XML
                let result;
                try {
                    result = await parser.parseStringPromise(response.data);
                } catch (parseError) {
                    console.error(`   ❌ Failed to parse XML from ${feedUrl}:`, parseError.message);
                    continue;
                }

                const items = result?.rss?.channel?.[0]?.item || result?.feed?.entry || [];

                console.log(`   Found ${items.length} items in RSS feed`);

                for (const item of items) {
                    try {
                        // Extract event data (RSS format varies, handle common patterns)
                        const title = (item.title?.[0]?._ || item.title?.[0] || "").trim();
                        const description = (item.description?.[0]?._ || item.description?.[0] || item.summary?.[0]?._ || item.summary?.[0] || "").trim();
                        const link = item.link?.[0]?._ || item.link?.[0]?.$?.href || item.link?.[0] || "";

                        // Validate required fields
                        if (!title || title.length < 3) {
                            continue; // Skip items without valid title
                        }

                        // Parse date (various RSS date formats)
                        let pubDate = item.pubDate?.[0] || item.published?.[0] || item.updated?.[0];
                        if (!pubDate) {
                            continue; // Skip items without date
                        }

                        let eventDate;
                        try {
                            eventDate = new Date(pubDate);
                            if (isNaN(eventDate.getTime())) {
                                console.warn(`   ⚠️ Invalid date format: ${pubDate}`);
                                continue;
                            }
                        } catch (dateError) {
                            console.warn(`   ⚠️ Error parsing date ${pubDate}:`, dateError.message);
                            continue;
                        }

                        // Accept events up to 2 weeks in the past or any future date
                        const daysPastThreshold = 14;
                        const threshold = Date.now() - (daysPastThreshold * 24 * 60 * 60 * 1000);
                        if (eventDate.getTime() < threshold) continue;

                        // Filter for toddler-appropriate events
                        const titleLower = title.toLowerCase();
                        const descLower = description.toLowerCase();
                        const isToddlerEvent = /toddler|baby|babies|infant|preschool|kids?|family|0-4|0-5|0 to 4|0 to 5|ages 0|under 5|storytime|story time|playgroup|music|movement|bounce|littles|play & learn/i.test(titleLower + " " + descLower);
                        if (!isToddlerEvent) continue;

                        // Extract venue information
                        const venueName = feedConfig.venue_name ||
                            title.match(/(?:at|@)\s+([^,]+)/i)?.[1]?.trim() ||
                            "Event Venue";
                        const city = feedConfig.city || "Bay Area, CA";

                        // Get coordinates
                        let coords = {
                            lat: feedConfig.latitude || null,
                            lng: feedConfig.longitude || null,
                        };

                        if (!coords.lat || !coords.lng) {
                            coords = await getDynamicCoordinates(`${venueName}, ${city}`, GOOGLE_MAPS_API_KEY.value());
                        }

                        // Validate coordinates
                        if (!coords || !coords.lat || !coords.lng || !validateCoordinates(coords.lat, coords.lng)) {
                            console.warn(`   ⚠️ Could not geocode or invalid coordinates: ${venueName}`);
                            continue;
                        }

                        const dateKey = `${eventDate.getFullYear()}-${String(eventDate.getMonth() + 1).padStart(2, "0")}-${String(eventDate.getDate()).padStart(2, "0")}`;
                        const uniqueId = generateEventId(title, venueName, dateKey);

                        const docRef = db.collection("activities").doc(uniqueId);
                        const docSnap = await docRef.get();

                        if (!docSnap.exists) {
                            const startSec = Math.floor(eventDate.getTime() / 1000);
                            const endSec = startSec + 60 * 60; // Default 1 hour (consistent with other functions)

                            // Generate geohash with error handling
                            let geohash;
                            try {
                                geohash = geofire.geohashForLocation([coords.lat, coords.lng]);
                            } catch (geohashErr) {
                                console.warn(`   ⚠️ Error generating geohash for ${venueName}:`, geohashErr.message);
                                continue;
                            }

                            await docRef.set({
                                title: title,
                                venue: venueName.trim(),
                                description: description || null,
                                startTime: startSec,
                                endTime: endSec,
                                ageRange: normalizeAgeRange(descLower) || "0-4 years",
                                isFree: true, // RSS feeds often don't specify, assume free
                                requiresBooking: /register|rsvp|sign up|ticket/i.test(descLower),
                                registrationUrl: link || null,
                                latitude: coords.lat,
                                longitude: coords.lng,
                                geohash: geohash,
                                position: {
                                    geohash: geohash,
                                    geopoint: new admin.firestore.GeoPoint(coords.lat, coords.lng),
                                },
                                location: {
                                    name: venueName,
                                    geohash: geohash,
                                    geopoint: new admin.firestore.GeoPoint(coords.lat, coords.lng),
                                },
                                sourceUrl: link || feedUrl,
                                createdAt: Math.floor(Date.now() / 1000),
                                expireAt: new Date(endSec * 1000 + 10 * 60 * 1000),
                                source: "rss_feed",
                                isIndoor: true,
                                type: "one_time",
                                category: "RSS Feed Event",
                            }, { merge: true });

                            totalAdded++;
                            console.log(`   ✅ Added: "${title.substring(0, 50)}" at ${venueName}`);
                        }
                    } catch (itemErr) {
                        console.warn(`   ⚠️ Error processing RSS item:`, itemErr.message);
                    }
                }

                // Small delay between feeds
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (feedErr) {
                console.error(`   ❌ Error processing RSS feed ${feedUrl}:`, feedErr.message);
            }
        }

        console.log(`🎉 RSS Feed Parser Complete: Processed ${totalProcessed} feeds, Added ${totalAdded} events`);
    },
);
