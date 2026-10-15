// Communico adapter.
//
// Many public libraries run their event calendars on Communico (<tenant>.libnet.info).
// Each library's public events page loads its events from a JSON endpoint (/eeventcaldata)
// that returns every event in a date range in one response, so a sync is one request per
// library. robots.txt allows it (it only disallows search pages). One adapter covers every
// library that uses it; adding a library is one entry in COMMUNICO_LIBRARIES. Every activity
// links back to the original event page.
//
// Events carry local times, age and tag labels, and the branch, but no coordinates, so each
// library's branches and the off-site spots it uses have fixed coordinates in its config.
// No LLM and no geocoding are needed.

const { sessionDuration } = require("../utils/duration");
const { decodeEntities, excerpt } = require("../utils/html");
const { CANCELLED_TITLE, classifyEvent } = require("../utils/libraryAudience");
const { pacificToSec, pacificDate } = require("../utils/pacificTime");

const USER_AGENT = "ToddleGoBot/0.1 (family events listing; links back to each event page)";

/**
 * Bay Area libraries whose event calendars run on Communico.
 * - tenant: the subdomain in https://<tenant>.libnet.info.
 * - websiteHosts: the library's own website, whose Gemini-scraped activities this replaces.
 * - branches: library buildings, matched by Communico location ID and name (an off-site
 *   event can carry a branch's ID with another place's name). The first is used for
 *   online events.
 * - places: off-site spots the library uses, tested against the event's location, venue
 *   name and title. An event at an off-site location not listed here is skipped rather than
 *   shown at a branch's address.
 * Coordinates were looked up from the street addresses (OpenStreetMap).
 */
const COMMUNICO_LIBRARIES = [
    {
        tenant: "berkeleypubliclibrary",
        websiteHosts: ["berkeleypubliclibrary.org"],
        branches: [
            { locationIds: ["4044"], match: /central/i, name: "Berkeley Public Library - Central", address: "2090 Kittredge St, Berkeley, CA 94704", lat: 37.867951, lng: -122.268897 },
            { locationIds: ["4046"], match: /north/i, name: "Berkeley Public Library - North Branch", address: "1170 The Alameda, Berkeley, CA 94707", lat: 37.885452, lng: -122.275438 },
            { locationIds: ["4047"], match: /south/i, name: "Berkeley Public Library - Tarea Hall Pittman South Branch", address: "1901 Russell St, Berkeley, CA 94703", lat: 37.856374, lng: -122.270997 },
            { locationIds: ["4049"], match: /west/i, name: "Berkeley Public Library - West Branch", address: "1125 University Ave, Berkeley, CA 94702", lat: 37.869503, lng: -122.290952 },
            { locationIds: ["4045"], match: /claremont/i, name: "Berkeley Public Library - Claremont Branch", address: "2940 Benvenue Ave, Berkeley, CA 94705", lat: 37.856566, lng: -122.254381 },
        ],
        places: [
            { match: /the freight|freight & salvage/i, name: "Freight & Salvage", address: "2020 Addison St, Berkeley, CA 94704", lat: 37.870764, lng: -122.269532, venueType: "music_venue" },
            { match: /frances albrier/i, name: "Frances Albrier Community Center", address: "2800 Park St, Berkeley, CA 94702", lat: 37.856516, lng: -122.28335, venueType: "community_center" },
            { match: /edible schoolyard/i, name: "Edible Schoolyard Berkeley", address: "1781 Rose St, Berkeley, CA 94703", lat: 37.882419, lng: -122.27771, venueType: "garden" },
            { match: /ywca/i, name: "YWCA Berkeley/Oakland", address: "2600 Bancroft Way, Berkeley, CA 94704", lat: 37.868766, lng: -122.256594, venueType: "community_center" },
        ],
    },
    {
        tenant: "beltib",
        websiteHosts: ["beltiblibrary.org"],
        branches: [
            { locationIds: ["3224"], match: /belvedere|tiburon/i, name: "Belvedere Tiburon Library", address: "1501 Tiburon Blvd, Tiburon, CA 94920", lat: 37.878654, lng: -122.463442 },
        ],
        places: [],
    },
];

/** Days of events requested per sync (the sync stores the next 60 days). */
const DAYS = 60;

// Communico's `changed` codes: 1 cancelled; 3 rescheduled, with the new session listed as
// its own event (new_event_id, changed = 0)
const CANCELLED_CODES = new Set(["1", "3"]);
const ONLINE_LOCATION = /^(virtual|online)$/i;

const yes = (v) => String(v) === "1";
const labels = (list) => (Array.isArray(list) ? list : [])
    .map((x) => decodeEntities(x).trim())
    .filter(Boolean);

/**
 * Where an event happens: a listed off-site place, the branch, the first branch for online
 * events, or null for an unlisted place, so the event is skipped instead of being shown at
 * the wrong address.
 * @returns {null|{name, address, lat, lng, venueType}}
 */
function placeFor(library, event) {
    const location = decodeEntities(event.location).trim();
    const venueName = decodeEntities(event.venue_name).trim();
    const title = decodeEntities(event.title);
    const place = (library.places || []).find((p) => p.match.test(`${location} | ${venueName} | ${title}`));
    if (place) return { venueType: "community_center", ...place };
    const id = String(event.location_id || "");
    const branch = library.branches.find((b) => b.locationIds.includes(id) && b.match.test(location));
    if (branch) return { venueType: "library", ...branch };
    if (isOnline(event)) return { venueType: "library", ...library.branches[0] };
    return null;
}

const isOnline = (event) =>
    String(event.event_type || "").toUpperCase() === "ONLINE" || ONLINE_LOCATION.test(String(event.location || "").trim());

/** Activity doc ID from Communico's event ID (each session of a recurring event has its own). */
const activityId = (tenant, eventId) => `communico_${tenant}_${eventId}`;

/**
 * Maps one Communico event to an activity, before venue and series are linked (the sync job
 * adds venueId, coordinates, seriesId and type, like the other jobs).
 * @returns {null|{activityId: string, activity: Object|null, venue?: Object, cancelled?: boolean}}
 *   null when the event is not for young children, private, has no valid time, or is at an
 *   unknown off-site place; { cancelled: true } when the library cancelled or moved it, so a
 *   saved copy can be removed.
 */
function buildActivity(library, event, { nowSec }) {
    const title = decodeEntities(event.title).trim();
    const id = activityId(library.tenant, event.id);
    if (CANCELLED_CODES.has(String(event.changed)) || CANCELLED_TITLE.test(title)) {
        return { activityId: id, activity: null, cancelled: true };
    }
    if (yes(event.private_event)) return null;

    const ages = labels(event.agesArray);
    const tags = labels(event.tagsArray);
    const audience = classifyEvent(ages, tags, title);
    if (!audience.include) return null;

    const startTime = pacificToSec(event.event_start || event.raw_start_time);
    if (!Number.isFinite(startTime)) return null;
    const { endTime, durationMinutes, durationEstimated } =
        sessionDuration(title, startTime, pacificToSec(event.event_end || event.raw_end_time));

    const place = placeFor(library, event);
    if (!place) return null;
    const isVirtual = isOnline(event);
    const room = decodeEntities(event.venue_room).trim();
    const requiresBooking = yes(event.allow_reg);
    const cost = Number(event.registration_cost) || 0;

    const activity = {
        title,
        venue: place.name,
        description: excerpt(event.long_description) || excerpt(event.description) || null,
        startTime,
        endTime,
        durationMinutes,
        durationEstimated,
        isAllDay: false,
        ageRange: audience.ageRange,
        age_range: audience.ageRangeYears,
        isFree: cost === 0,
        cost: cost > 0 ? `$${cost}` : null,
        requiresBooking,
        registrationUrl: requiresBooking ? (yes(event.third_party_reg) && event.reg_url) || event.url : null,
        locationDetails: room || null,
        isVirtual,
        isIndoor: !isVirtual && place.venueType === "library",
        sourceUrl: event.url,
        source: "communico",
        sourceEventId: String(event.id),
        tenant: library.tenant,
        venueType: place.venueType,
        tags: [...new Set([...ages, ...tags])],
        category: "Library Program",
        lastVerifiedAt: nowSec,
        // Remove from the app shortly after the event ends (Firestore TTL on expireAt)
        expireAt: new Date((endTime + 10 * 60) * 1000),
    };
    const venue = { name: place.name, address: place.address, lat: place.lat, lng: place.lng };
    return { activityId: id, activity, venue };
}

/**
 * Fetches a library's events for the next DAYS days, starting today (Pacific), in one
 * request.
 * @returns {Promise<{events: Object[], complete: boolean}>}
 */
async function fetchAllEvents(library, { nowSec = Math.floor(Date.now() / 1000), days = DAYS, fetchImpl = fetch } = {}) {
    const req = JSON.stringify({ private: false, date: pacificDate(nowSec), days, locations: [], ages: [], types: [] });
    const url = `https://${library.tenant}.libnet.info/eeventcaldata?event_type=0&req=${encodeURIComponent(req)}`;
    const res = await fetchImpl(url, {
        headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
        signal: AbortSignal.timeout(60000),
    });
    if (!res.ok) throw new Error(`${library.tenant} events: HTTP ${res.status}`);
    let body;
    try {
        body = JSON.parse(await res.text());
    } catch (e) {
        // e.g. a "Site Maintenance" HTML page
        throw new Error(`${library.tenant} events: not JSON`);
    }
    if (!Array.isArray(body)) throw new Error(`${library.tenant} events: unexpected response`);
    const seen = new Set();
    const events = body.filter((e) => e && e.id && !seen.has(e.id) && seen.add(e.id));
    return { events, complete: true };
}

/**
 * Whether an activity scraped from `url` (by the Gemini jobs) is covered by this library's
 * Communico calendar: the URL is on its Communico site or its own website.
 */
function isSupersededUrl(url, { tenant, websiteHosts = [] }) {
    let host;
    try {
        host = new URL(url).host.toLowerCase().replace(/^www\./, "");
    } catch (e) {
        return false;
    }
    if (host === `${tenant}.libnet.info`) return true;
    return websiteHosts.some((h) => host === h || host.endsWith(`.${h}`));
}

module.exports = {
    COMMUNICO_LIBRARIES,
    USER_AGENT,
    placeFor,
    activityId,
    buildActivity,
    fetchAllEvents,
    isSupersededUrl,
};
