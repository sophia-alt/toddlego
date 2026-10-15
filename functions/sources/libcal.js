// LibCal (Springshare) adapter.
//
// Many public libraries run their event calendars on LibCal. Each library's calendar page
// loads its events from a public JSON endpoint (/ajax/calendar/list), which also backs the
// RSS and iCal feeds LibCal offers for other sites to subscribe to. One adapter covers every
// library that uses it; adding a library is one entry in LIBCAL_LIBRARIES. LibCal's
// robots.txt asks for a 10-second crawl delay, which is respected between pages, and every
// activity links back to the original event page.
//
// Events carry local times, audience and category labels, branch ("campus") and room, but no
// coordinates, so each library's branches and its recurring off-site spots (park storytimes)
// have fixed coordinates in its config. No LLM and no geocoding are needed.

const { sessionDuration } = require("../utils/duration");
const { decodeEntities, excerpt } = require("../utils/html");
const { CANCELLED_TITLE, classifyEvent } = require("../utils/libraryAudience");
const { pacificToSec } = require("../utils/pacificTime");

const USER_AGENT = "ToddleGoBot/0.1 (family events listing; links back to each event page)";

/** LibCal's robots.txt: Crawl-delay: 10. */
const CRAWL_DELAY_SEC = 10;

/**
 * Bay Area libraries whose event calendars run on LibCal.
 * - tenant: the subdomain in https://<tenant>.libcal.com.
 * - calendarIds: calendars to read; others (e.g. bookmobile stop schedules) are skipped.
 * - websiteHosts / websitePaths: the library's own website (a whole host, or a host and path
 *   prefix on a city website), whose Gemini-scraped activities this replaces.
 * - branches: library buildings; the first is used when an event names no other.
 *   `match` is tested against the event's campus and location.
 * - places: recurring off-site spots, tested against the event's location and title.
 *   An event at an off-site location not listed here is skipped rather than shown at the
 *   library's address.
 * Coordinates were looked up from the street addresses (OpenStreetMap).
 */
const LIBCAL_LIBRARIES = [
    {
        tenant: "mountainview",
        calendarIds: [8800], // Not 8985, the bookmobile's stop schedule
        websitePaths: ["mountainview.gov/our-city/departments/library"],
        branches: [
            { name: "Mountain View Public Library", address: "585 Franklin St, Mountain View, CA 94041", lat: 37.39032, lng: -122.08339 },
        ],
        places: [
            { match: /pioneer park/i, name: "Pioneer Park, Mountain View", address: "Church St & Franklin St, Mountain View, CA 94041", lat: 37.389538, lng: -122.08317, venueType: "park" },
            { match: /cuesta park/i, name: "Cuesta Park, Mountain View", address: "615 Cuesta Dr, Mountain View, CA 94040", lat: 37.372602, lng: -122.082028, venueType: "park" },
            { match: /magical bridge|rengstorff/i, name: "Rengstorff Park, Mountain View", address: "201 S Rengstorff Ave, Mountain View, CA 94040", lat: 37.400206, lng: -122.09635, venueType: "park" },
            { match: /deer hollow/i, name: "Deer Hollow Farm", address: "22500 Cristo Rey Dr, Cupertino, CA 95014", lat: 37.332627, lng: -122.100153, venueType: "farm" },
        ],
    },
    {
        tenant: "burlingame",
        calendarIds: [6897],
        websitePaths: ["burlingame.org/library"],
        branches: [
            { name: "Burlingame Public Library", address: "480 Primrose Rd, Burlingame, CA 94010", lat: 37.579285, lng: -122.349173 },
            { match: /easton/i, name: "Burlingame Public Library - Easton Branch", address: "1800 Easton Dr, Burlingame, CA 94010", lat: 37.583825, lng: -122.371638 },
        ],
        places: [],
    },
    {
        tenant: "millvalleylibrary",
        calendarIds: [17002],
        websiteHosts: ["millvalleylibrary.org"],
        branches: [
            { name: "Mill Valley Public Library", address: "375 Throckmorton Ave, Mill Valley, CA 94941", lat: 37.906444, lng: -122.554784 },
        ],
        places: [
            { match: /boyle/i, name: "Boyle Park, Mill Valley", address: "E Manor Dr, Mill Valley, CA 94941", lat: 37.905224, lng: -122.538342, venueType: "park" },
            { match: /hauke/i, name: "Hauke Park, Mill Valley", address: "Hamilton Dr, Mill Valley, CA 94941", lat: 37.896541, lng: -122.523699, venueType: "park" },
            { match: /sweetwater/i, name: "Sweetwater Music Hall", address: "19 Corte Madera Ave, Mill Valley, CA 94941", lat: 37.906933, lng: -122.548013, venueType: "music_venue" },
        ],
    },
    {
        tenant: "sanleandro",
        calendarIds: [14130],
        websitePaths: ["sanleandro.org/314/library"],
        branches: [
            { name: "San Leandro Public Library", address: "300 Estudillo Ave, San Leandro, CA 94577", lat: 37.726036, lng: -122.154055 },
        ],
        places: [],
    },
    {
        tenant: "sanmateopublic",
        // Children's calendar only; the adult and teen calendar (12176) has nothing for under-5s
        calendarIds: [16089],
        websiteHosts: ["smplibrary.org"],
        websitePaths: ["cityofsanmateo.org/4086/library"],
        branches: [
            { name: "San Mateo Public Library", address: "55 W 3rd Ave, San Mateo, CA 94402", lat: 37.562017, lng: -122.326719 },
            { match: /hillsdale/i, name: "San Mateo Public Library - Hillsdale Branch", address: "205 W Hillsdale Blvd, San Mateo, CA 94403", lat: 37.533454, lng: -122.304738 },
            { match: /marina/i, name: "San Mateo Public Library - Marina Branch", address: "1530 Susan Ct, San Mateo, CA 94403", lat: 37.556011, lng: -122.293524 },
        ],
        places: [],
    },
    {
        tenant: "larkspurlibrary",
        calendarIds: [22992],
        websiteHosts: ["larkspurlibrary.org"],
        branches: [
            { name: "Larkspur Library", address: "400 Magnolia Ave, Larkspur, CA 94939", lat: 37.934286, lng: -122.53558 },
        ],
        places: [],
    },
];

/** Locations that are away from the library; see `places` in LIBCAL_LIBRARIES. */
const OFFSITE = /\boff-?site\b|\bpark\b|\bfarm\b/i;
/** Locations that say nothing about the room ("Online", "San Mateo Main", "Main Library"). */
const NO_ROOM = /^(online|off-?site|library-wide|(.+ )?main( library)?)$/i;

const labelNames = (list) => (list || [])
    .map((x) => decodeEntities(typeof x === "string" ? x : x && x.name).trim())
    .filter(Boolean);

/**
 * Where an event happens: a listed off-site place, a branch, or (for off-site locations not
 * listed) null, so the event is skipped instead of being shown at the wrong address.
 * @returns {null|{name, address, lat, lng, venueType}}
 */
function placeFor(library, event) {
    const location = decodeEntities(event.location).trim();
    const campus = decodeEntities(event.campus).trim();
    const title = decodeEntities(event.title);
    const place = (library.places || []).find((p) => p.match.test(`${location} | ${title}`));
    if (place) return { venueType: "park", ...place };
    const branch = library.branches.find((b) => b.match && b.match.test(`${campus} | ${location}`));
    if (branch) return { venueType: "library", ...branch };
    if (OFFSITE.test(location)) return null;
    return { venueType: "library", ...library.branches[0] };
}

/** The room or spot within a place, or null when the location only repeats the place. */
function roomDetails(location, place) {
    if (!location || NO_ROOM.test(location)) return null;
    return place.name.toLowerCase().includes(location.toLowerCase()) ? null : location;
}

/** Activity doc ID from LibCal's event ID (each session of a recurring event has its own). */
const activityId = (tenant, eventId) => `libcal_${tenant}_${eventId}`;

/**
 * Maps one LibCal event to an activity, before venue and series are linked (the sync job
 * adds venueId, coordinates, seriesId and type, like the other jobs).
 * @returns {null|{activityId: string, activity: Object|null, venue?: Object, cancelled?: boolean}}
 *   null when the event is not for young children, has no valid time, or is at an unknown
 *   off-site location; { cancelled: true } when the library marked it cancelled, so a saved
 *   copy can be removed.
 */
function buildActivity(library, event, { nowSec }) {
    const title = decodeEntities(event.title).trim();
    const id = activityId(library.tenant, event.id);
    if (CANCELLED_TITLE.test(title)) return { activityId: id, activity: null, cancelled: true };

    const audiences = labelNames(event.audiences);
    const categories = labelNames(event.categories_arr);
    const audience = classifyEvent(audiences, categories, title);
    if (!audience.include) return null;

    const startTime = pacificToSec(event.startdt);
    if (!Number.isFinite(startTime)) return null;
    const isAllDay = !!event.all_day;
    const { endTime, durationMinutes, durationEstimated } = sessionDuration(title, startTime, pacificToSec(event.enddt));

    const isVirtual = !!event.online_event;
    const place = placeFor(library, event);
    if (!place) return null;
    const location = decodeEntities(event.location).trim();
    const requiresBooking = !!(event.registration_enabled || event.online_registration || event.in_person_registration);
    const cost = decodeEntities(event.registration_cost).trim();

    const activity = {
        title,
        venue: place.name,
        description: excerpt(event.description) || excerpt(event.shortdesc) || null,
        startTime,
        endTime,
        durationMinutes,
        durationEstimated,
        isAllDay,
        ageRange: audience.ageRange,
        age_range: audience.ageRangeYears,
        // Public library programs; a few charge a materials fee
        isFree: !cost || /^(free|\$?0(\.00)?)$/i.test(cost),
        cost: cost || null,
        requiresBooking,
        registrationUrl: requiresBooking ? event.url : null,
        locationDetails: roomDetails(location, place),
        isVirtual,
        isIndoor: !isVirtual && place.venueType === "library",
        sourceUrl: event.url,
        source: "libcal",
        sourceEventId: String(event.id),
        tenant: library.tenant,
        venueType: place.venueType,
        tags: [...new Set([...audiences, ...categories])],
        category: "Library Program",
        lastVerifiedAt: nowSec,
        // Remove from the app shortly after the event ends (Firestore TTL on expireAt)
        expireAt: new Date((endTime + 10 * 60) * 1000),
    };
    const venue = { name: place.name, address: place.address, lat: place.lat, lng: place.lng };
    return { activityId: id, activity, venue };
}

/**
 * Fetches every page of a library's upcoming events (LibCal lists them from today on), one
 * page at a time with the crawl delay between requests, keeping the configured calendars.
 * @returns {Promise<{events: Object[], complete: boolean}>} complete is false when the page
 *   cap stopped the read early, so callers do not treat unlisted events as removed.
 */
async function fetchAllEvents(library, { perPage = 48, maxPages = 20, fetchImpl = fetch, sleep } = {}) {
    const wait = sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
    const calendars = new Set(library.calendarIds);
    const events = [];
    const seen = new Set();
    let read = 0;
    for (let page = 1; page <= maxPages; page++) {
        if (page > 1) await wait(CRAWL_DELAY_SEC * 1000);
        const url = `https://${library.tenant}.libcal.com/ajax/calendar/list?c=-1&date=0000-00-00` +
            `&perpage=${perPage}&page=${page}&audience=&cats=&camps=&inc=0`;
        const res = await fetchImpl(url, {
            headers: { "User-Agent": USER_AGENT, Accept: "application/json", "X-Requested-With": "XMLHttpRequest" },
            signal: AbortSignal.timeout(30000),
        });
        if (!res.ok) throw new Error(`${library.tenant} events page ${page}: HTTP ${res.status}`);
        const body = await res.json();
        const results = Array.isArray(body.results) ? body.results : [];
        read += results.length;
        for (const e of results) {
            if (e && e.id && !seen.has(e.id) && calendars.has(e.calendar_id)) {
                seen.add(e.id);
                events.push(e);
            }
        }
        const total = Number(body.total_results);
        if (results.length < perPage || (Number.isFinite(total) && read >= total)) return { events, complete: true };
    }
    return { events, complete: false };
}

/**
 * Whether an activity scraped from `url` (by the Gemini jobs) is covered by this library's
 * LibCal calendar: the URL is on its LibCal site or its own website. City websites are
 * matched by path, so the city's other events are kept.
 */
function isSupersededUrl(url, { tenant, websiteHosts = [], websitePaths = [] }) {
    let parsed;
    try {
        parsed = new URL(url);
    } catch (e) {
        return false;
    }
    const host = parsed.host.toLowerCase().replace(/^www\./, "");
    if (host === `${tenant}.libcal.com`) return true;
    if (websiteHosts.some((h) => host === h || host.endsWith(`.${h}`))) return true;
    const hostPath = `${host}${parsed.pathname}`.toLowerCase();
    return websitePaths.some((p) => hostPath === p || hostPath.startsWith(`${p}/`));
}

module.exports = {
    LIBCAL_LIBRARIES,
    USER_AGENT,
    CRAWL_DELAY_SEC,
    classifyEvent,
    pacificToSec,
    placeFor,
    activityId,
    buildActivity,
    fetchAllEvents,
    isSupersededUrl,
};
