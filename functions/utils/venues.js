const admin = require("firebase-admin");
const crypto = require("crypto");
const geofire = require("geofire-common");
const { db, getDynamicCoordinates, validateCoordinates } = require("./helpers");

/**
 * Venue entities: one doc per physical place in `venues`, with fixed coordinates.
 * Activities reference a venue by `venueId` instead of geocoding their venue text each time.
 *
 * A venue is found by any of its known spellings (`aliasKeys`), so a name seen before never
 * costs a geocode again. New spellings that geocode to the same Google place join that venue.
 */

/**
 * Lookup key for a venue name: lowercase, "&" as "and", punctuation dropped, single spaces.
 * "Alameda County Library – Dublin" and "alameda county library, dublin" share a key.
 */
const normalizeVenueName = (name) =>
    String(name)
        .toLowerCase()
        .replace(/&/g, " and ")
        .replace(/[^\p{L}\p{N}]+/gu, " ")
        .trim();

const sha = (s) => crypto.createHash("sha256").update(s, "utf8").digest("hex");

/**
 * Coordinates a source config has for its own venue, passed as resolveVenue's knownCoords only
 * for events at that venue. A library system's page lists events at many branches; giving
 * every branch the page's coordinates would save new venues at the wrong place, and every
 * later source with that venue name would reuse them.
 * @returns {Object|null} `coords` when the names match (ignoring case/punctuation), else null.
 */
function knownCoordsFor(eventVenueName, configVenueName, coords) {
    if (!coords || !eventVenueName || !configVenueName) return null;
    return normalizeVenueName(eventVenueName) === normalizeVenueName(configVenueName) ? coords : null;
}

/** Venue doc ID: from the Google place when known, otherwise from the normalized name. */
const venueIdFor = (placeId, venueKey) =>
    placeId ? `place_${sha(placeId).substring(0, 32)}` : `name_${sha(venueKey).substring(0, 32)}`;

/**
 * Finds or creates the venue for a venue name.
 * @param {string} name - Venue text from the source.
 * @param {Object} opts
 * @param {string} opts.apiKey - Google Maps key, used only when the venue is new.
 * @param {Object} [opts.budget] - Run budget from createRunBudget().
 * @param {Map} [opts.memo] - Per-run cache of venueKey -> venue (or null for misses).
 * @param {{lat: number, lng: number, address?: string}} [opts.knownCoords] - Coordinates the
 *   source config already has; used instead of geocoding when the venue is new.
 * @returns {Promise<{venueId: string, name: string, lat: number, lng: number, geohash: string}|null>}
 */
async function resolveVenue(name, { apiKey, budget, memo, knownCoords } = {}) {
    const venueKey = name ? normalizeVenueName(name) : "";
    if (!venueKey) return null;
    if (memo && memo.has(venueKey)) return memo.get(venueKey);

    const venue = await findOrCreateVenue(String(name).trim(), venueKey, { apiKey, budget, knownCoords });
    if (memo) memo.set(venueKey, venue);
    return venue;
}

async function findOrCreateVenue(name, venueKey, { apiKey, budget, knownCoords }) {
    const known = await db.collection("venues").where("aliasKeys", "array-contains", venueKey).limit(1).get();
    if (!known.empty) return toVenue(known.docs[0]);

    const coords = knownCoords && validateCoordinates(knownCoords.lat, knownCoords.lng)
        ? { ...knownCoords, placeId: null }
        : await getDynamicCoordinates(name, apiKey, budget);
    if (!coords || !validateCoordinates(coords.lat, coords.lng)) return null;

    const venueRef = db.collection("venues").doc(venueIdFor(coords.placeId, venueKey));
    const existing = await venueRef.get();
    if (existing.exists) {
        // Another spelling of a place we already have: keep its coordinates, remember this name.
        await venueRef.update({
            aliasKeys: admin.firestore.FieldValue.arrayUnion(venueKey),
            aliases: admin.firestore.FieldValue.arrayUnion(name),
        });
        return toVenue(existing);
    }

    const geohash = geofire.geohashForLocation([coords.lat, coords.lng]);
    const data = {
        name,
        address: coords.address || null,
        placeId: coords.placeId || null,
        latitude: coords.lat,
        longitude: coords.lng,
        geohash,
        geopoint: new admin.firestore.GeoPoint(coords.lat, coords.lng),
        aliasKeys: [venueKey],
        aliases: [name],
        coordinateSource: knownCoords ? "source_config" : "geocoding",
        createdAt: Math.floor(Date.now() / 1000),
    };
    await venueRef.set(data);
    console.log(`🏠 New venue ${venueRef.id}: ${name}`);
    return toVenue({ id: venueRef.id, data: () => data });
}

const toVenue = (doc) => {
    const d = doc.data();
    return { venueId: doc.id, name: d.name, lat: d.latitude, lng: d.longitude, geohash: d.geohash };
};

/** Location fields every activity stores, copied from its venue so geohash queries still work. */
const venueLocationFields = (venue, displayName) => ({
    venueId: venue.venueId,
    latitude: venue.lat,
    longitude: venue.lng,
    geohash: venue.geohash,
    position: {
        geohash: venue.geohash,
        geopoint: new admin.firestore.GeoPoint(venue.lat, venue.lng),
    },
    location: {
        name: displayName || venue.name,
        geohash: venue.geohash,
        geopoint: new admin.firestore.GeoPoint(venue.lat, venue.lng),
    },
});

module.exports = {
    normalizeVenueName,
    knownCoordsFor,
    venueIdFor,
    resolveVenue,
    venueLocationFields,
};
