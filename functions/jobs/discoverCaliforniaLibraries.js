const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("firebase-admin");
const { Client } = require("@googlemaps/google-maps-services-js");
const { db } = require("../utils/helpers");
const { GOOGLE_MAPS_API_KEY } = require("../utils/secrets");

const mapsClient = new Client({});

/** Max cities to process per invocation to stay under timeout (Places API + Firestore). */
const MAX_CITIES_PER_RUN = 20;

/** Delay (ms) between Place Details calls to avoid rate limits. */
const PLACE_DETAILS_DELAY_MS = 150;

/**
 * Sanitize a string for use as a Firestore document ID (no forward slashes; matches legacy format).
 * @param {string} website - Raw URL or string.
 * @returns {string} Safe doc ID (forward slash removed; same length as legacy for compatibility).
 */
function safeUrlDocId(website) {
    if (!website || typeof website !== "string") return "";
    const base64 = Buffer.from(website).toString("base64").replace(/\//g, "").substring(0, 100);
    return base64 || "unknown";
}

/**
 * Discovery Function: Monthly Scout
 * Searches for public libraries and parks in California cities using Google Places API
 * Stores discovered websites in url_registry for the daily scraper to process
 * Runs monthly (1st at 00:00 UTC). Processes up to MAX_CITIES_PER_RUN per run to avoid timeout.
 */
exports.discoverCaliforniaLibraries = onSchedule(
    {
        schedule: "0 0 1 * *", // 1st of month at midnight UTC (monthly)
        secrets: [GOOGLE_MAPS_API_KEY],
        timeoutSeconds: 300,
        memory: "256MiB",
    },
    async (event) => {
        const runStartMs = Date.now();
        console.log(`[discovery] START at ${new Date().toISOString()} (runId: ${runStartMs})`);

        // Pull cities from config_cities collection
        const fetchCitiesStart = Date.now();
        const citiesSnap = await db.collection("config_cities").get();
        console.log(`[discovery] Firestore config_cities get took ${Date.now() - fetchCitiesStart}ms, size=${citiesSnap.size}`);

        if (citiesSnap.empty) {
            console.log("ℹ️ No cities found in config_cities. Seed with import_ca_cities.py.");
            return;
        }

        // Process oldest-scanned or pending cities first, cap per run to avoid timeout
        const docs = citiesSnap.docs;
        const sorted = docs.slice().sort((a, b) => {
            const aData = a.data();
            const bData = b.data();
            const aTs = aData.last_scanned?.toMillis?.() ?? 0;
            const bTs = bData.last_scanned?.toMillis?.() ?? 0;
            if (aData.status === "pending" && bData.status !== "pending") return -1;
            if (aData.status !== "pending" && bData.status === "pending") return 1;
            return aTs - bTs;
        });
        const toProcess = sorted.slice(0, MAX_CITIES_PER_RUN);
        console.log(`[discovery] Processing ${toProcess.length} of ${docs.length} cities (max ${MAX_CITIES_PER_RUN} per run)`);

        let totalDiscovered = 0;
        let totalRegistered = 0;

        for (let i = 0; i < toProcess.length; i++) {
            const cityDoc = toProcess[i];
            const cityRef = cityDoc.ref;
            const cityData = cityDoc.data() || {};
            const cityName = cityData.name || `${cityDoc.id.replace(/_/g, " ")}, CA`;
            const cityStartMs = Date.now();
            console.log(`[discovery] City ${i + 1}/${toProcess.length}: ${cityName} (docId: ${cityDoc.id})`);

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

                for (let q = 0; q < queries.length; q++) {
                    const query = queries[q];
                    const queryStartMs = Date.now();
                    const response = await mapsClient.textSearch({
                        params: {
                            query,
                            key: GOOGLE_MAPS_API_KEY.value(),
                        },
                    });
                    console.log(`[discovery] Places textSearch "${query.substring(0, 40)}..." took ${Date.now() - queryStartMs}ms`);

                    const results = response.data.results || [];
                    discoveredForCity += results.length;
                    console.log(`📍 Found ${results.length} results for ${cityName} (${query})`);

                    for (let p = 0; p < results.length; p++) {
                        const place = results[p];
                        if (p > 0) {
                            await new Promise((r) => setTimeout(r, PLACE_DETAILS_DELAY_MS));
                        }
                        try {
                            const detailStartMs = Date.now();
                            const details = await mapsClient.placeDetails({
                                params: {
                                    place_id: place.place_id,
                                    fields: ["name", "website", "geometry"],
                                    key: GOOGLE_MAPS_API_KEY.value(),
                                },
                            });
                            if (p === 0 || p === results.length - 1) {
                                console.log(`[discovery] placeDetails(${place.place_id}) took ${Date.now() - detailStartMs}ms`);
                            }

                            const website = details.data?.result?.website;
                            const name = details.data?.result?.name || place.name;
                            const loc = details.data?.result?.geometry?.location;

                            if (website && loc) {
                                const urlDocId = safeUrlDocId(website);
                                if (!urlDocId) {
                                    console.warn(`[discovery] Skipping empty docId for website: ${website.substring(0, 50)}`);
                                    continue;
                                }
                                const writeStartMs = Date.now();
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
                                if (registeredForCity < 3) {
                                    console.log(`[discovery] Firestore url_registry set took ${Date.now() - writeStartMs}ms`);
                                }
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

                    await new Promise((r) => setTimeout(r, 500));
                }

                totalDiscovered += discoveredForCity;

                await cityRef.set(
                    {
                        status: "complete",
                        last_scanned: admin.firestore.FieldValue.serverTimestamp(),
                        libraries_found: registeredForCity,
                        queries_run: queries.length,
                    },
                    { merge: true }
                );
                console.log(`[discovery] City ${cityName} completed in ${Date.now() - cityStartMs}ms (registered ${registeredForCity})`);
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
                console.log(`[discovery] City ${cityName} error after ${Date.now() - cityStartMs}ms`);
            }
        }

        const runDurationMs = Date.now() - runStartMs;
        console.log(`[discovery] END at ${new Date().toISOString()}, duration=${runDurationMs}ms, discovered=${totalDiscovered}, registered=${totalRegistered}`);
        console.log(`🎉 Discovery Complete: Found ${totalDiscovered} places, Registered ${totalRegistered} new URLs`);
    },
);
