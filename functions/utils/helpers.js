const admin = require("firebase-admin");
const { Client } = require("@googlemaps/google-maps-services-js");
const crypto = require("crypto");
const geofire = require("geofire-common");

// Initialize Firebase Admin if not already initialized
if (!admin.apps.length) {
    admin.initializeApp();
}

const mapsClient = new Client({});
const db = admin.firestore();

/**
 * Helper to check if cached coordinates are still fresh (less than 6 months old)
 */
const isCoordinatesCacheFresh = (timestamp) => {
    if (!timestamp) return false;
    const sixMonthsAgo = Date.now() - 180 * 24 * 60 * 60 * 1000;
    return timestamp > sixMonthsAgo;
};

/**
 * Dynamically fetches coordinates for any venue string using Google Maps.
 * Uses a local Firestore cache to minimize API calls.
 * @param {string} venueName - The name of the library or place.
 * @param {string} apiKey - Your Google Maps API Key.
 * @returns {Promise<Object>} { lat, lng, formattedAddress }
 */
async function getDynamicCoordinates(venueName, apiKey) {
    if (!venueName || typeof venueName !== 'string') {
        return { lat: null, lng: null, address: null };
    }

    // Normalize venue name and generate cache ID
    const normalizedName = String(venueName).trim().toLowerCase();
    if (!normalizedName) {
        return { lat: null, lng: null, address: null };
    }

    // Check cache first
    // Use a safe encoding that avoids special characters in Firestore doc IDs
    const cacheId = Buffer.from(normalizedName).toString("base64")
        .replace(/[\/+=]/g, '') // Remove characters that might cause issues
        .substring(0, 50);
    const cacheRef = db.collection("geocoding_cache").doc(cacheId);
    const cacheDoc = await cacheRef.get();

    if (cacheDoc.exists) {
        const cached = cacheDoc.data();
        if (isCoordinatesCacheFresh(cached.cachedAt)) {
            console.log(`✅ Cache hit for ${normalizedName}`);
            return {
                lat: cached.lat,
                lng: cached.lng,
                address: cached.address,
            };
        }
    }

    // Cache miss or expired - call API
    try {
        console.log(`🔎 Geocoding miss for ${normalizedName}`);

        // Add rate limiting: delay before geocoding to avoid hitting API limits
        await new Promise(resolve => setTimeout(resolve, 200)); // 200ms delay

        // Use original venueName for geocoding (preserves formatting that might help API)
        const geocodeQuery = typeof venueName === 'string' ? venueName.trim() : String(venueName);

        // First try with California constraint
        let response = await mapsClient.geocode({
            params: {
                address: `${geocodeQuery}, California`,
                key: apiKey,
            },
        });

        // If no results with California constraint, try without it (fallback)
        if (response.data.results.length === 0) {
            console.warn(
                `⚠️ No results for "${geocodeQuery}, California" - retrying without location constraint`,
            );
            response = await mapsClient.geocode({
                params: {
                    address: geocodeQuery,
                    key: apiKey,
                },
            });
        }

        if (response.data.results.length > 0) {
            const result = response.data.results[0];
            const address = result.formatted_address;
            const lat = result.geometry.location.lat;
            const lng = result.geometry.location.lng;
            const inCaliforniaArea =
                lat >= 32.5 && lat <= 42 && lng <= -114 && lng >= -124;

            if (!inCaliforniaArea) {
                console.warn(
                    `⚠️ Geocoded address is likely outside California: ${address}`,
                );
            }

            // Store in cache
            await cacheRef.set(
                {
                    venueName: normalizedName,
                    lat: lat,
                    lng: lng,
                    address: address,
                    cachedAt: Date.now(),
                },
                { merge: true }
            );

            console.log(`📍 Geocoded ${normalizedName}: ${address}`);
            return {
                lat: lat,
                lng: lng,
                address: address,
            };
        }
    } catch (error) {
        const errorName = typeof venueName === 'string' ? venueName : 'unknown';
        console.error(
            `[Geocoding Error] Could not find: ${errorName}`,
            error.message,
        );
    }

    return { lat: null, lng: null, address: null };
}

/**
 * Validate coordinates are valid for saving to Firestore
 * @param {number} lat - Latitude
 * @param {number} lng - Longitude
 * @returns {boolean} True if coordinates are valid
 */
function validateCoordinates(lat, lng) {
    // Check if coordinates exist
    if (lat == null || lng == null || lat === undefined || lng === undefined) {
        return false;
    }

    // Check if coordinates are not the default (0, 0)
    if (lat === 0.0 && lng === 0.0) {
        return false;
    }

    // Validate coordinate ranges
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
        return false;
    }

    // Check if coordinates are valid numbers
    if (isNaN(lat) || isNaN(lng) || !isFinite(lat) || !isFinite(lng)) {
        return false;
    }

    return true;
}

/**
 * Helper to create a unique, URL-safe ID for each event.
 * Using Title + Venue + Date ensures recurring events are saved separately.
 */
const generateEventId = (title, venue, date) => {
    // Validate inputs
    if (!title || !venue || !date) {
        throw new Error("generateEventId requires title, venue, and date");
    }
    const rawStr = `${String(title)}-${String(venue)}-${String(date)}`.toLowerCase().replace(/\s+/g, "-");
    // Remove special characters that might cause issues in Firestore doc IDs
    const safeStr = rawStr.replace(/[^a-z0-9\-]/g, '');
    return Buffer.from(safeStr).toString("base64")
        .replace(/[\/+=]/g, '') // Remove base64 padding characters
        .substring(0, 50);
};

/**
 * Normalize various age labels into consistent ranges for the client
 */
const normalizeAgeRange = (input) => {
    if (!input) return null;
    const s = String(input).toLowerCase();
    if (/baby|babies|infant/.test(s)) return "0-18 months";
    if (/toddler/.test(s)) return "18-36 months";
    if (/preschool/.test(s)) return "3-5 years";
    if (/all ages/.test(s)) return "All";
    const match = s.match(/(\d+)\s*-\s*(\d+)\s*(months|month|years|year|y)/);
    if (match) {
        const start = match[1];
        const end = match[2];
        const unit = /month/.test(match[3]) ? "months" : "years";
        return `${start}-${end} ${unit}`;
    }
    return null;
};

/**
 * Check if an ISO date is more than 2 weeks in the past (relaxed from original)
 * This allows backfilling of recent events and accepts events up to 2 weeks old
 */
const isPastIsoDate = (iso, daysPastThreshold = 14) => {
    const t = Date.parse(iso);
    if (isNaN(t)) return true;
    const threshold = Date.now() - (daysPastThreshold * 24 * 60 * 60 * 1000);
    return t < threshold;
};

/**
 * Parse date string into Date object, handling various formats
 * @returns {Date|null} Parsed date or null if invalid
 */
const parseDate = (dateStr) => {
    if (!dateStr || ["TBD", "ongoing", "TBA"].includes(dateStr)) return null;

    // Check if date string includes time (has T or space followed by time pattern)
    const hasTime = /T\d{2}:\d{2}/.test(dateStr) || /\s+\d{1,2}:\d{2}/.test(dateStr);

    let eventDate = new Date(dateStr);

    if (isNaN(eventDate.getTime())) {
        // Try parsing various formats
        const monthDayYear = dateStr.match(/(\w+)\s+(\d+),?\s+(\d{4})/i);
        if (monthDayYear) {
            eventDate = new Date(`${monthDayYear[1]} ${monthDayYear[2]}, ${monthDayYear[3]}`);
        }

        const slashDate = dateStr.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
        if (slashDate && isNaN(eventDate.getTime())) {
            const year = slashDate[3].length === 2 ? `20${slashDate[3]}` : slashDate[3];
            eventDate = new Date(`${year}-${slashDate[1].padStart(2, '0')}-${slashDate[2].padStart(2, '0')}`);
        }

        const monthDay = dateStr.match(/(\w+)\s+(\d+)/i);
        if (monthDay && isNaN(eventDate.getTime())) {
            const currentYear = new Date().getFullYear();
            eventDate = new Date(`${monthDay[1]} ${monthDay[2]}, ${currentYear}`);
            if (eventDate.getTime() < Date.now() - 7 * 24 * 60 * 60 * 1000) {
                eventDate = new Date(`${monthDay[1]} ${monthDay[2]}, ${currentYear + 1}`);
            }
        }

        if (isNaN(eventDate?.getTime())) {
            return null;
        }
    }

    // If date was parsed without time, set to noon (12:00 PM) local time to avoid timezone issues
    // This is safer than midnight which can cause timezone conversion problems
    if (!hasTime) {
        // Get date components and set to noon local time
        const year = eventDate.getFullYear();
        const month = eventDate.getMonth();
        const day = eventDate.getDate();
        eventDate = new Date(year, month, day, 12, 0, 0); // Noon local time
    }

    return eventDate;
};

/**
 * Check if a date is valid and within acceptable range (up to 2 weeks in the past for backfilling)
 * @returns {boolean} True if date is valid and within range
 */
const isValidFutureDate = (dateStr, daysPastThreshold = 14) => {
    const eventDate = parseDate(dateStr);
    if (!eventDate) return false;

    // Accept dates up to 2 weeks in the past (for backfilling) or any future date
    const threshold = Date.now() - (daysPastThreshold * 24 * 60 * 60 * 1000);
    return eventDate.getTime() >= threshold;
};

/**
 * Helper to generate SHA-256 hash of content
 */
const generateContentHash = (content) => {
    return crypto.createHash("sha256").update(content, "utf8").digest("hex");
};

/**
 * Clean markdown content by removing dynamic elements before hashing.
 * This prevents false cache misses from timestamps, ads, session IDs, etc.
 */
const cleanContentForHashing = (markdown) => {
    // Remove common dynamic patterns that change on every page load
    let cleaned = markdown
        // Remove timestamps and dates (various formats)
        .replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/g, "")
        .replace(/\d{1,2}\/\d{1,2}\/\d{2,4}/g, "")
        .replace(
            /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b/gi,
            "",
        )
        // Remove "Last updated" sections
        .replace(/last\s+updated:?\s*[^\n]*/gi, "")
        .replace(/updated\s+on:?\s*[^\n]*/gi, "")
        // Remove session/tracking IDs (common patterns)
        .replace(/sessionid[=:]\s*[a-zA-Z0-9]+/gi, "")
        .replace(/trackingid[=:]\s*[a-zA-Z0-9]+/gi, "")
        .replace(/\b[a-f0-9]{32,64}\b/g, "") // MD5/SHA hashes
        // Remove query parameters that might be dynamic
        .replace(/\?[^\s\]]+/g, "")
        // Normalize whitespace
        .replace(/\s+/g, " ")
        .trim();

    return cleaned;
};

/**
 * Save activity to Firestore with deduplication
 */
async function saveActivity(activityData, db) {
    try {
        const { title, venue, startTime } = activityData;
        if (!title || !venue || !startTime) {
            console.warn("⚠️ Skipping activity with missing required fields:", activityData);
            return false;
        }

        // Generate unique ID
        const eventDate = new Date(startTime * 1000);
        const dateKey = `${eventDate.getFullYear()}-${String(eventDate.getMonth() + 1).padStart(2, "0")}-${String(eventDate.getDate()).padStart(2, "0")}`;
        const uniqueId = generateEventId(title, venue, dateKey);

        const docRef = db.collection("activities").doc(uniqueId);
        const docSnap = await docRef.get();

        // Only add if new
        if (!docSnap.exists) {
            // Add geohash if coordinates available
            if (activityData.latitude && activityData.longitude) {
                activityData.geohash = geofire.geohashForLocation([
                    activityData.latitude,
                    activityData.longitude,
                ]);
                activityData.position = {
                    geohash: activityData.geohash,
                    geopoint: new admin.firestore.GeoPoint(
                        activityData.latitude,
                        activityData.longitude,
                    ),
                };
                activityData.location = {
                    name: venue,
                    geohash: activityData.geohash,
                    geopoint: new admin.firestore.GeoPoint(
                        activityData.latitude,
                        activityData.longitude,
                    ),
                };
            }

            // Ensure expireAt is set
            if (!activityData.expireAt) {
                const endSec = activityData.endTime || (startTime + 2 * 60 * 60);
                activityData.expireAt = new Date(endSec * 1000 + 10 * 60 * 1000);
            }

            await docRef.set(activityData, { merge: true });
            return true;
        }
        return false;
    } catch (error) {
        console.error("❌ Error saving activity:", error.message);
        return false;
    }
}

module.exports = {
    getDynamicCoordinates,
    generateEventId,
    normalizeAgeRange,
    isPastIsoDate,
    parseDate,
    isValidFutureDate,
    generateContentHash,
    cleanContentForHashing,
    saveActivity,
    validateCoordinates,
    db,
};
