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

// Calendar entries that are not activities: closures ("Holiday -- Library Closed",
// "CLOSED: Veterans Day", "Libraries Close @ 5PM")
const NOT_AN_ACTIVITY = /\bclosed\b|closure|\blibrar(y|ies) closes?\b/i;
const CANCELLED_TITLE = /^(cancel+ed|postponed)\b/i;
// Audience, category and title words for young children. "Babytime" and "Baby (0-18 mos)"
// count, so no word boundary after "baby".
const BABY_TODDLER = /\bbab(y|ies)|toddler|infant|lapsit/i;
const PRESCHOOL = /pre-?school/i;
const STORYTIME = /story ?time|cuentos|rhyme ?time/i;
const FAMILY = /\bfamil(y|ies)\b/i;
// Labels for school-age or grown-up audiences ("Grades 1-6", "Children (grades K-5)",
// "Tweens", "Adults 18+"). They exclude an event only when it has no young-child label.
const OLDER_AUDIENCE = /grade|\bK\s?-\s?\d|tween|teen|middle school|high school|adult|senior/i;
const SCHOOL_AGE_TITLE = /homework|after[- ]?school|tutor|\btweens?\b|\bteens?\b|grades?\s*\d|\bK\s?-\s?\d/i;
// An age span in a title, e.g. "Storytime for Ages 2-5", "Tween LEGO Club (Ages 9-13)"
const AGE_SPAN = /\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b/i;
/** Locations that are away from the library; see `places` in LIBCAL_LIBRARIES. */
const OFFSITE = /\boff-?site\b|\bpark\b|\bfarm\b/i;
/** Locations that say nothing about the room ("Online", "San Mateo Main", "Main Library"). */
const NO_ROOM = /^(online|off-?site|library-wide|(.+ )?main( library)?)$/i;

/** Youngest age ToddleGo is for is 0; programs whose minimum age is above this are left out. */
const MAX_MIN_AGE = 5;

const labelNames = (list) => (list || [])
    .map((x) => decodeEntities(typeof x === "string" ? x : x && x.name).trim())
    .filter(Boolean);

/**
 * Decides whether an event suits children 0-5, and the age range to show.
 * @param {string[]} audiences - LibCal audience names ("Babies", "Toddler (19 mos-3 yrs)").
 * @param {string[]} categories - LibCal category names ("Storytime", "Children's Program").
 * @param {string} title - Decoded title.
 * @returns {{include: boolean, ageRange: string|null, ageRangeYears: number[]|null}}
 */
function classifyEvent(audiences, categories, title) {
    const no = { include: false, ageRange: null, ageRangeYears: null };
    const labels = [...audiences, ...categories];
    if (labels.some((l) => NOT_AN_ACTIVITY.test(l)) || NOT_AN_ACTIVITY.test(title)) return no;

    const text = [...labels, title];
    const baby = text.some((t) => BABY_TODDLER.test(t));
    const preschool = text.some((t) => PRESCHOOL.test(t));

    const span = title.match(AGE_SPAN);
    if (span) {
        const [min, max] = [Number(span[1]), Number(span[2])];
        if (min > MAX_MIN_AGE || max < min) return no;
        if (!baby && !preschool && audiences.some((a) => OLDER_AUDIENCE.test(a))) return no; // "Ages 5-11", Grades 1-6
        return { include: true, ageRange: `${min}-${max} years`, ageRangeYears: [min, max] };
    }
    if (baby && preschool) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (baby) return { include: true, ageRange: "0-3 years", ageRangeYears: [0, 3] };
    if (preschool) return { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] };

    // No young-child label: school-age and grown-up programs are left out
    if (SCHOOL_AGE_TITLE.test(title)) return no;
    const family = audiences.some((a) => FAMILY.test(a));
    if (!family && audiences.some((a) => OLDER_AUDIENCE.test(a)) && !audiences.some((a) => /child/i.test(a))) return no;

    // Library storytimes without a stated age are for babies to preschoolers
    if (text.some((t) => STORYTIME.test(t))) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (family) return { include: true, ageRange: "All", ageRangeYears: null };
    // "Children" alone covers school-age clubs and classes, so it is not enough
    return no;
}

/** Offset of Pacific time from UTC (minutes, e.g. -420) at a moment. */
function pacificOffsetMinutes(ms) {
    const parts = Object.fromEntries(new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        hourCycle: "h23",
        year: "numeric", month: "2-digit", day: "2-digit",
        hour: "2-digit", minute: "2-digit", second: "2-digit",
    }).formatToParts(new Date(ms)).map((p) => [p.type, p.value]));
    const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
    return Math.round((asUtc - ms) / 60000);
}

/**
 * "2026-10-15 16:00:00" (Pacific local time, as LibCal returns it) to Unix seconds,
 * independent of the process timezone.
 */
function pacificToSec(s) {
    const m = String(s || "").match(/^(\d{4})-(\d\d)-(\d\d)[ T](\d\d):(\d\d)(?::(\d\d))?/);
    if (!m) return NaN;
    const wall = Date.UTC(m[1], m[2] - 1, m[3], m[4], m[5], m[6] || 0);
    let ms = wall - pacificOffsetMinutes(wall) * 60000;
    // Near a DST switch the first guess can use the other side's offset
    const offset = pacificOffsetMinutes(ms);
    ms = wall - offset * 60000;
    return Math.floor(ms / 1000);
}

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
