const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("firebase-admin");
const { Client } = require("@googlemaps/google-maps-services-js");
const { db } = require("../utils/helpers");
const { GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

const mapsClient = new Client({});

/**
 * Discovery Function: Monthly Scout
 * Searches for public libraries and parks in California cities using Google Places API
 * Stores discovered websites in url_registry for the daily scraper to process
 * Runs monthly (1st at 00:00 UTC) to balance data freshness with Places API cost
 */
exports.discoverCaliforniaLibraries = onSchedule(
    {
        schedule: "0 0 1 * *", // 1st of month at midnight UTC (monthly)
        secrets: [GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 120,
        memory: "256MiB",
    },
    async (event) => {
        console.log("🔍 Starting California Library & Park Discovery (config_cities)...");

        // Pull cities from config_cities collection
        const citiesSnap = await db.collection("config_cities").get();
        if (citiesSnap.empty) {
            console.log("ℹ️ No cities found in config_cities. Seed with import_ca_cities.py.");
            return;
        }

        let totalDiscovered = 0;
        let totalRegistered = 0;

        for (const cityDoc of citiesSnap.docs) {
            const cityRef = cityDoc.ref;
            const cityData = cityDoc.data() || {};
            const cityName = cityData.name || `${cityDoc.id.replace(/_/g, " ")}, CA`;

            console.log(`🔍 Searching for libraries and parks in ${cityName}...`);

            // Mark scanning start
            await cityRef.set(
                {
                    status: "scanning",
                    last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                },
                { merge: true }
            );

            try {
                const queries = [
                    `public library in ${cityName}`,
                    `park in ${cityName}`,
                ];

                let registeredForCity = 0;
                let discoveredForCity = 0;

                for (const query of queries) {
                    const response = await mapsClient.textSearch({
                        params: {
                            query,
                            key: GOOGLE_MAPS_API_KEY.value(),
                        },
                    });

                    const results = response.data.results || [];
                    discoveredForCity += results.length;
                    console.log(`📍 Found ${results.length} results for ${cityName} (${query})`);

                    for (const place of results) {
                        try {
                            const details = await mapsClient.placeDetails({
                                params: {
                                    place_id: place.place_id,
                                    fields: ["name", "website", "geometry"],
                                    key: GOOGLE_MAPS_API_KEY.value(),
                                },
                            });

                            const website = details.data?.result?.website;
                            const name = details.data?.result?.name || place.name;
                            const loc = details.data?.result?.geometry?.location;

                            if (website && loc) {
                                const urlDocId = Buffer.from(website)
                                    .toString("base64")
                                    .substring(0, 100);

                                await db.collection("url_registry").doc(urlDocId).set(
                                    {
                                        url_hash: website,
                                        venue_name: name,
                                        city: cityName,
                                        latitude: loc.lat,
                                        longitude: loc.lng,
                                        last_discovered: admin.firestore.FieldValue.serverTimestamp(),
                                    },
                                    { merge: true }
                                );

                                registeredForCity++;
                                totalRegistered++;
                                console.log(`✅ Registered: ${name} (${website})`);
                            }
                        } catch (placeError) {
                            const detailMessage =
                                placeError?.response?.data?.error_message ||
                                placeError?.message ||
                                placeError;
                            console.error(
                                `⚠️ Error getting details for place: ${place.name} (${place.place_id})`,
                                detailMessage
                            );
                        }
                    }

                    // Small delay between queries
                    await new Promise(resolve => setTimeout(resolve, 500));
                }

                totalDiscovered += discoveredForCity;

                // Mark city as complete
                await cityRef.set(
                    {
                        status: "complete",
                        last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                        libraries_found: registeredForCity,
                        queries_run: queries.length,
                    },
                    { merge: true }
                );
            } catch (error) {
                const errorMessage =
                    error?.response?.data?.error_message ||
                    error?.response?.data?.error?.message ||
                    error?.message ||
                    error;

                console.error(`❌ Error searching ${cityName}:`, errorMessage);
                await cityRef.set(
                    {
                        status: "error",
                        last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                        error_message: String(errorMessage),
                        error_status: error?.response?.status || null,
                    },
                    { merge: true }
                );
            }
        }

        console.log(`🎉 Discovery Complete: Found ${totalDiscovered} places, Registered ${totalRegistered} new URLs`);
    },
);
