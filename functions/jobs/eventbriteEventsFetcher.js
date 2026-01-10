const { onSchedule } = require("firebase-functions/v2/scheduler");
const axios = require("axios");
const geofire = require("geofire-common");
const admin = require("firebase-admin");
const {
    db,
    getDynamicCoordinates,
    generateEventId,
} = require("../utils/helpers");
const { GOOGLE_MAPS_API_KEY, EVENTBRITE_API_KEY } = require("../utils/secrets");

/**
 * Eventbrite API Events Fetcher (Weekly)
 * Fetches toddler-friendly events from Eventbrite (FREE API)
 * 2,500 requests/day free tier - more than enough for our use case
 * Runs every Sunday at 4 AM UTC
 */
exports.eventbriteEventsFetcher = onSchedule(
    {
        schedule: "0 4 * * 0", // Every Sunday at 4 AM UTC
        secrets: [GOOGLE_MAPS_API_KEY, EVENTBRITE_API_KEY],
        timeoutSeconds: 300,
        memory: "512MiB",
    },
    async (event) => {
        console.log("🎫 Starting Eventbrite events fetch...");

        // Check if Eventbrite API key is configured
        let eventbriteKey;
        try {
            eventbriteKey = EVENTBRITE_API_KEY.value();
        } catch (e) {
            console.log("ℹ️ Eventbrite API key not configured. Skipping Eventbrite fetch.");
            console.log("   To enable: Add EVENTBRITE_API_KEY to Firebase secrets.");
            console.log("   Get API key: https://www.eventbrite.com/platform/api-keys/");
            return;
        }

        // Bay Area locations for searching events
        const bayAreaLocations = [
            { name: "San Francisco", lat: 37.7749, lng: -122.4194 },
            { name: "Oakland", lat: 37.8044, lng: -122.2712 },
            { name: "San Jose", lat: 37.3382, lng: -121.8863 },
            { name: "Fremont", lat: 37.5483, lng: -121.9886 },
            { name: "Hayward", lat: 37.6688, lng: -122.0810 },
            { name: "Berkeley", lat: 37.8715, lng: -122.2730 },
            { name: "Richmond", lat: 37.9358, lng: -122.3477 },
            { name: "Concord", lat: 37.9780, lng: -122.0311 },
        ];

        let totalAdded = 0;

        for (const location of bayAreaLocations) {
            try {
                // Eventbrite API endpoint for searching events
                // API Documentation: https://www.eventbrite.com/platform/api-reference/event-search/
                const response = await axios.get("https://www.eventbriteapi.com/v3/events/search/", {
                    params: {
                        "location.latitude": location.lat,
                        "location.longitude": location.lng,
                        "location.within": "25mi",
                        "q": "toddler OR baby OR preschool OR infant OR storytime",
                        "categories": "113", // Family & Education category
                        "sort_by": "date",
                        "status": "live",
                        "expand": "venue",
                        "page_size": 50, // Max results per request
                    },
                    headers: {
                        "Authorization": `Bearer ${eventbriteKey}`,
                    },
                    timeout: 10000, // 10 second timeout
                });

                if (!response.data) {
                    console.warn(`   ⚠️ No data in Eventbrite API response for ${location.name}`);
                    continue;
                }

                const events = response.data.events || [];
                if (!Array.isArray(events)) {
                    console.warn(`   ⚠️ Unexpected Eventbrite API response format for ${location.name}`);
                    continue;
                }

                console.log(`   📍 Found ${events.length} events near ${location.name}`);

                for (const eventbriteEvent of events) {
                    try {
                        // Validate event structure
                        if (!eventbriteEvent.name || !eventbriteEvent.name.text) {
                            continue; // Skip events without valid name
                        }

                        // Filter for toddler-appropriate events
                        const description = (eventbriteEvent.description?.text || "").toLowerCase();
                        const title = (eventbriteEvent.name.text || "").toLowerCase();

                        const isToddlerEvent = /toddler|baby|babies|infant|preschool|0-4|0 to 4|ages 0|under 5|storytime|playgroup/i.test(title + " " + description);
                        if (!isToddlerEvent) continue;

                        if (!eventbriteEvent.start || !eventbriteEvent.start.utc) {
                            continue; // Skip events without start date
                        }

                        const startDate = new Date(eventbriteEvent.start.utc);
                        if (isNaN(startDate.getTime())) {
                            console.warn(`   ⚠️ Invalid start date for event: ${title}`);
                            continue; // Skip invalid dates
                        }

                        // Accept events up to 2 weeks in the past (for backfilling) or any future date
                        const daysPastThreshold = 14;
                        const threshold = Date.now() - (daysPastThreshold * 24 * 60 * 60 * 1000);
                        if (startDate.getTime() < threshold) {
                            continue; // Too far in the past
                        }

                        const venue = eventbriteEvent.venue;
                        if (!venue || !venue.name || !venue.name.text) {
                            continue; // Skip events without valid venue information
                        }
                        const venueName = venue.name.text;
                        const venueAddress = venue.address?.localized_area_display || "";

                        // Geocode venue if coordinates not available
                        let coords = {
                            lat: venue?.latitude || null,
                            lng: venue?.longitude || null,
                        };

                        if (!coords.lat || !coords.lng) {
                            const geocoded = await getDynamicCoordinates(
                                `${venueName}, ${venueAddress}`,
                                GOOGLE_MAPS_API_KEY.value()
                            );
                            coords = { lat: geocoded.lat, lng: geocoded.lng };
                        }

                        if (!coords.lat || !coords.lng) {
                            // Try fallback with city name
                            coords = await getDynamicCoordinates(
                                `${location.name}, California`,
                                GOOGLE_MAPS_API_KEY.value()
                            );
                        }

                        if (!coords.lat || !coords.lng) continue;

                            const dateKey = `${startDate.getFullYear()}-${String(startDate.getMonth() + 1).padStart(2, "0")}-${String(startDate.getDate()).padStart(2, "0")}`;
                            const eventTitle = eventbriteEvent.name.text.trim();
                            if (!eventTitle || eventTitle.length < 3) {
                                continue; // Skip events with invalid titles
                            }
                            const uniqueId = generateEventId(eventTitle, venueName, dateKey);

                        const docRef = db.collection("activities").doc(uniqueId);
                        const docSnap = await docRef.get();

                        if (!docSnap.exists) {
                            const startSec = Math.floor(startDate.getTime() / 1000);
                            const endDate = new Date(eventbriteEvent.end?.utc || startDate.getTime() + 60 * 60 * 1000);
                            const endSec = Math.floor(endDate.getTime() / 1000);

                            // Check if event is free
                            const isFree = eventbriteEvent.is_free === true ||
                                (eventbriteEvent.ticket_availability?.minimum_ticket_price?.value === 0);

                            const geohash = geofire.geohashForLocation([coords.lat, coords.lng]);

                            await docRef.set({
                                title: eventTitle,
                                venue: venueName.trim(),
                                description: eventbriteEvent.description?.text || null,
                                startTime: startSec,
                                endTime: endSec,
                                ageRange: "0-4 years",
                                isFree: isFree,
                                requiresBooking: true, // Eventbrite events typically require registration
                                registrationUrl: eventbriteEvent.url || null,
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
                                sourceUrl: eventbriteEvent.url || null,
                                createdAt: Math.floor(Date.now() / 1000),
                                expireAt: new Date(endSec * 1000 + 10 * 60 * 1000),
                                source: "eventbrite",
                                isIndoor: true,
                                type: "one_time",
                                category: "Eventbrite Event",
                            }, { merge: true });

                            totalAdded++;
                            console.log(`   ✅ Added: "${eventbriteEvent.name.text}" at ${venueName}`);
                        }
                    } catch (eventErr) {
                        console.warn(`   ⚠️ Error processing Eventbrite event:`, eventErr.message);
                    }
                }

                // Delay between locations
                await new Promise(resolve => setTimeout(resolve, 2000));
            } catch (e) {
                console.error(`❌ Eventbrite error for ${location.name}:`, e?.message || e);
            }
        }

        console.log(`🎯 Eventbrite events fetch complete. New events added: ${totalAdded}`);
    },
);
