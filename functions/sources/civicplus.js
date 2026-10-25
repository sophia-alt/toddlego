// CivicPlus calendar adapter, for city and park district event calendars.
//
// Many California cities and park districts run their websites on CivicPlus, whose calendar
// offers a public iCalendar subscription per category (the "iCalendar" / "Notify Me" links on
// each site's Calendar.aspx). One adapter covers every such site; adding one is an entry in
// CIVICPLUS_CITIES with the calendar categories that hold public events. robots.txt on these
// sites allows the feed. Every activity links back to the original event page.
//
// City calendars mix public events with council meetings, senior programs and closures, and
// carry no audience labels, so an event is kept only when its title names young children or
// families (storytimes, Pre-K, playground events, trunk-or-treats, festivals). Locations are
// free text ("Oak Hill Park & Community Center > Ballroom - 3005 Stone Valley Rd  Danville CA
// 94526"), so each new venue is geocoded once and found by name after that.
//
// Sites that answer our requests with HTTP 403 are left out rather than worked around.

const { sessionDuration } = require("../utils/duration");
const { excerpt } = require("../utils/html");
const { parseEvents } = require("../utils/ical");
const { classifyEvent: classifyLibraryEvent } = require("../utils/libraryAudience");

const USER_AGENT = "ToddleGoBot/0.1 (family events listing; links back to each event page)";

/**
 * Bay Area cities and park districts whose calendars run on CivicPlus.
 * - town: the city, for venue names and geocoding when an address does not name one.
 * - categories: calendar category IDs (catID) with public events; meeting, closure and
 *   commission calendars are left out. A category is skipped when another adapter covers it
 *   (e.g. Dublin's library is Alameda County Library, read from BiblioCommons).
 * - familyCategories: categories of public events the city puts on for the community
 *   (festivals, holiday lights, playground pop-ups). Their events are kept unless the title
 *   says they are for grown-ups, older children or pets; in other categories the title has to
 *   name young children or families.
 * - crawlDelaySec: pause between category feeds.
 */
const CIVICPLUS_CITIES = [
    {
        id: "haywardrec",
        town: "Hayward",
        name: "Hayward Area Recreation and Park District",
        baseUrl: "https://www.haywardrec.org",
        categories: { 14: "Main Calendar", 31: "Special Events", 35: "Arts" },
        familyCategories: [31],
    },
    {
        id: "sanmateo",
        town: "San Mateo",
        name: "City of San Mateo",
        baseUrl: "https://www.cityofsanmateo.org",
        // The library's calendars are read from LibCal (sanmateopublic)
        categories: { 15: "Parks and Recreation Events", 20: "Front Page Events Listing", 51: "Central Park Music Series" },
        familyCategories: [15],
    },
    {
        id: "burlingame",
        town: "Burlingame",
        name: "City of Burlingame",
        baseUrl: "https://www.burlingame.org",
        // The library's calendars are read from LibCal (burlingame)
        categories: { 14: "Main Calendar", 23: "Parks" },
    },
    {
        id: "campbell",
        town: "Campbell",
        name: "City of Campbell",
        baseUrl: "https://www.campbellca.gov",
        categories: { 29: "Recreation & Community Services", 47: "Campbell Community Event Calendar" },
    },
    {
        id: "losgatos",
        town: "Los Gatos",
        name: "Town of Los Gatos",
        baseUrl: "https://www.losgatosca.gov",
        categories: { 18: "Library" },
    },
    {
        id: "saratoga",
        town: "Saratoga",
        name: "City of Saratoga",
        baseUrl: "https://www.saratoga.ca.us",
        categories: { 35: "Community Events" },
    },
    {
        id: "danville",
        town: "Danville",
        name: "Town of Danville",
        baseUrl: "https://www.danville.ca.gov",
        categories: { 24: "Recreation Events", 25: "Community Events", 31: "Art, Theatre & Culture" },
        familyCategories: [24],
    },
    {
        id: "dalycity",
        town: "Daly City",
        name: "City of Daly City",
        baseUrl: "https://www.dalycity.org",
        categories: { 14: "Main Calendar", 22: "Library", 27: "Recreation" },
    },
    {
        id: "napa",
        town: "Napa",
        name: "City of Napa",
        baseUrl: "https://www.cityofnapa.org",
        categories: { 35: "Parks and Recreation Special Events", 54: "City Events" },
    },
    {
        id: "sfrecpark",
        town: "San Francisco",
        name: "San Francisco Recreation and Parks",
        baseUrl: "https://sfrecpark.org",
        categories: {
            14: "Main Calendar", 22: "Golden Gate Park", 30: "Mobile Rec", 37: "JFK Promenade Calendar",
            39: "EcoCenter Public Programs", 42: "Sunset Dunes",
        },
        // Mobile Rec brings play equipment and games to parks and street fairs
        familyCategories: [30],
    },
    {
        id: "dublin",
        town: "Dublin",
        name: "City of Dublin",
        baseUrl: "https://www.dublin.ca.gov",
        // The library (29) is Alameda County Library, read from BiblioCommons
        categories: { 14: "Main City Calendar", 23: "Parks & Community Services" },
    },
    {
        id: "morganhill",
        town: "Morgan Hill",
        name: "City of Morgan Hill",
        baseUrl: "https://www.morganhill.ca.gov",
        // The government calendar also lists Santa Clara County Library events (BiblioCommons)
        categories: { 44: "Centennial Recreation Center Calendar", 46: "Parks & Recreation Calendar" },
    },
].map((c) => ({ crawlDelaySec: 2, familyCategories: [], ...c }));

const CANCELLED = /\bcancel+ed\b|\bpostponed\b/i;
// Calendar entries that are not public activities for families
const NOT_AN_ACTIVITY = /\bmeeting\b|commission|committee|\bcouncil\b|\bboard\b|hearing|office hours|\bclosed\b|closure|deadline|^last day\b|volunteer|clean-?up|blood drive|evaluation/i;
// Programs for grown-ups and older children ("Golden Age Matinee" is for seniors), and
// drop-off nights ("Parents' Night Out")
const GROWN_UP = /\badults?\b|senior|golden age|\b(50|55|18|21)\s*\+|\bteens?\b|\btweens?\b|grades?\s*(?:[6-9]|1[0-2])\b|\bwine\b|\bbrews?\b|\bbeer\b|cocktail|happy hour|improv|comedy|book club|support group|caregiver|night out/i;
// Title words for young children or families. "Kids' Indoor Play Space", "Pre-K: Gratitude
// Gathering", "Trunk or Treat", "Photos with Santa at Magical Bridge", "Crawl-O-Ween", ...
const BABY_TODDLER = /\bbab(y|ies)\b|toddler|infant/i;
const PRE_K = /\bpre-?k\b|pre-?school/i;
const FAMILY_FRIENDLY = new RegExp([
    "\\bfamil(y|ies)\\b", "\\bkids?\\b", "\\bchild(ren)?\\b", "story ?time", "playground", "play ?(space|group)",
    "trunk[- ]or[- ]treat", "pumpkin", "\\bsanta\\b", "halloween", "o-?ween\\b", "spooktacular", "\\bboo at\\b",
    "egg hunt", "parade", "festival", "carnival", "puppet", "mobile rec", "d[ií]a de (los )?muertos",
    "day of the dead", "tree lighting", "lighting of the",
].join("|"), "i");
// Events for pets ("Howl-O-Ween - Dog Costume Contest and Party")
const PETS = /\bdogs?\b|\bpets?\b|canine/i;
// Programs for people with disabilities are usually for teens and adults unless they say kids
const ADAPTIVE = /\badaptive\b/i;
// An age span, e.g. "Elf Workshop (1-9 Yrs)", "Ages 2-5"
const AGE_SPAN = /\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b|\b(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\s*(?:yrs?|years?)\b/i;

/** Youngest age ToddleGo is for is 0; programs whose minimum age is above this are left out. */
const MAX_MIN_AGE = 5;

/**
 * Decides whether a city calendar event suits young children, and the age range to show.
 * Only the title and category are used: CivicPlus feeds carry no audience labels.
 * @param {string} title
 * @param {{familyCategory?: boolean}} [opts] - familyCategory: the event is in one of the
 *   city's familyCategories, so it is kept unless the title rules it out.
 * @returns {{include: boolean, ageRange: string|null, ageRangeYears: number[]|null}}
 */
function classifyEvent(title, { familyCategory = false } = {}) {
    const no = { include: false, ageRange: null, ageRangeYears: null };
    if (NOT_AN_ACTIVITY.test(title) || GROWN_UP.test(title) || PETS.test(title)) return no;
    if (ADAPTIVE.test(title) && !/\bkids?\b|\bchild/i.test(title)) return no;

    const span = title.match(AGE_SPAN);
    if (span) {
        const [min, max] = (span[1] ? [span[1], span[2]] : [span[3], span[4]]).map(Number);
        if (min > MAX_MIN_AGE || max < min) return no;
        return { include: true, ageRange: `${min}-${max} years`, ageRangeYears: [min, max] };
    }
    const baby = BABY_TODDLER.test(title);
    const preK = PRE_K.test(title);
    if (baby && preK) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (preK) return { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] };
    // Storytimes, "Baby Bounce", "Family Storytime": the library rules know their ages
    const library = classifyLibraryEvent([], [], title);
    if (library.include) return library;
    if (familyCategory || FAMILY_FRIENDLY.test(title)) return { include: true, ageRange: "All", ageRangeYears: null };
    return no;
}

const clean = (s) => excerpt(s, 200).replace(/\s+/g, " ").trim();

/**
 * Splits a CivicPlus location into the place, the room and the street address.
 * "Oak Hill Park & Community Center > Ballroom - 3005 Stone Valley Rd  Danville CA 94526"
 * gives place "Oak Hill Park & Community Center", room "Ballroom", address "3005 Stone
 * Valley Rd Danville CA 94526". The place can be empty (" - Mission St and Avalon Avenue ...")
 * or HTML ("<p>Civic Center Plaza</p> - 335 McAllister St, ...").
 * @returns {{place: string, room: string|null, address: string, town: string|null}}
 */
function parseLocation(location) {
    // The separator can start the text when the place is empty ("- Mission St ...")
    const text = ` ${String(location || "")}`;
    const sep = text.indexOf(" - ");
    const [placePart, addressPart] = sep >= 0 ? [text.slice(0, sep), text.slice(sep + 3)] : [text, ""];
    const [place, ...rooms] = clean(placePart).split(/\s+>\s+/);
    // The city follows the street after two or more spaces: "420 Front Street  Danville CA"
    const town = addressPart.match(/\s{2,}([A-Za-z][A-Za-z .]*?)\s+CA\b/);
    return { place: place || "", room: rooms.join(", ") || null, address: clean(addressPart), town: town ? town[1].trim() : null };
}

// Place names that are outdoors
const OUTDOOR = /\bpark\b|playground|plaza|grove|dunes|\bfield\b|garden|trail|beach|\bzoo\b|downtown|promenade|parking lot|\bfarm\b|\bstreets?\b|\bave(nue)?\b|\bblvd\b|\bway\b/i;

/**
 * The venue an event is at, to geocode when new: the place's name, or its street address
 * when the place has no name. Null when the location gives neither (the event is skipped).
 * Venues are found by name across all sources, so a name without its city gets one
 * ("Kennedy Park, Hayward"): otherwise every city's "Community Center" would be one venue.
 */
function eventVenue(city, location) {
    const { place, room, address, town: addressTown } = parseLocation(location);
    // A place name over a sentence long is a pasted description, not a name
    const placeName = /[a-z]/i.test(place) && place.length <= 80 ? place : "";
    if (!placeName && !/\d|[a-z]{3}/i.test(address)) return null;
    const town = addressTown || city.town;
    // Judged on the place alone: a town name can have "Park" in it (Menlo Park)
    const outdoor = !room && OUTDOOR.test(placeName || address);
    if (!placeName) return { name: address.replace(/,?\s+CA\b.*$/, ""), room, outdoor, geocodeQuery: address };
    const name = placeName.toLowerCase().includes(town.toLowerCase()) ? placeName : `${placeName}, ${town}`;
    return { name, room, outdoor, geocodeQuery: address ? `${placeName}, ${address}` : `${name}, CA` };
}

/** Activity doc ID from the site's own event ID (each session has its own). */
const activityId = (cityId, uid) => `civicplus_${cityId}_${uid}`;

/**
 * Maps one iCalendar event to an activity, before venue and series are linked.
 * @param {Object} city - Entry from CIVICPLUS_CITIES.
 * @param {Object} event - From ical.parseEvents(), with `category` (the category's name) and
 *   `categoryId`.
 * @returns {null|{activityId, activity, venue, cancelled?}} null when the event is not for
 *   young children or cannot be placed; { cancelled: true } when the city marked it
 *   cancelled, so a saved copy can be removed.
 */
function buildActivity(city, event, { nowSec }) {
    const title = clean(event.summary);
    if (!title || !/^\d+$/.test(event.uid)) return null;
    const id = activityId(city.id, event.uid);
    if (CANCELLED.test(title) || event.status === "CANCELLED") return { activityId: id, activity: null, cancelled: true };

    const familyCategory = city.familyCategories.includes(Number(event.categoryId));
    const audience = classifyEvent(title, { familyCategory });
    if (!audience.include) return null;
    if (!Number.isFinite(event.start)) return null;
    const venue = eventVenue(city, event.location);
    if (!venue) return null;

    // An all-day event's DTEND is the next day; show it as the day it is on
    const listedEnd = event.allDay ? event.start + 24 * 60 * 60 - 60 : event.end;
    const { endTime, durationMinutes, durationEstimated } = sessionDuration(title, event.start, listedEnd);
    // The description is either the event page's URL or the event's text
    const description = /^https?:\/\/\S+$/.test(event.description) ? null : excerpt(event.description) || null;
    const library = /librar/i.test(venue.name) || /librar/i.test(event.category || "");

    const activity = {
        title,
        venue: venue.name,
        description,
        startTime: event.start,
        endTime,
        durationMinutes,
        durationEstimated,
        isAllDay: event.allDay,
        ageRange: audience.ageRange,
        age_range: audience.ageRangeYears,
        // City calendars do not list prices; only say free when the title does
        isFree: /\bfree\b/i.test(title),
        cost: null,
        requiresBooking: false,
        registrationUrl: null,
        locationDetails: venue.room,
        isVirtual: false,
        isIndoor: !venue.outdoor,
        sourceUrl: `${city.baseUrl}/Calendar.aspx?EID=${event.uid}`,
        source: "civicplus",
        sourceEventId: event.uid,
        tenant: city.id,
        venueType: library ? "library" : venue.outdoor ? "park" : "community_center",
        tags: event.category ? [event.category] : [],
        category: library ? "Library Program" : "City Event",
        lastVerifiedAt: nowSec,
        // Remove from the app shortly after the event ends (Firestore TTL on expireAt)
        expireAt: new Date((endTime + 10 * 60) * 1000),
    };
    return { activityId: id, activity, venue: { name: venue.name, geocodeQuery: venue.geocodeQuery } };
}

const feedUrl = (city, catId) => `${city.baseUrl}/common/modules/iCalendar/iCalendar.aspx?catID=${catId}&feed=calendar`;

/**
 * Reads every category feed of a city, one at a time with the crawl delay between them.
 * An event listed in two categories is kept once, under the first; family categories are
 * read first, so an event in one of them is judged as such.
 * @returns {Promise<{events: Object[], complete: boolean}>} complete is false when a feed
 *   failed, so callers do not treat its events as removed.
 */
async function fetchAllEvents(city, { fetchImpl = fetch, sleep } = {}) {
    const wait = sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
    const events = [];
    const seen = new Set();
    let complete = true;
    let first = true;
    let failures = 0;
    const isFamily = ([id]) => city.familyCategories.includes(Number(id));
    const all = Object.entries(city.categories);
    const categories = [...all.filter(isFamily), ...all.filter((c) => !isFamily(c))];
    for (const [catId, category] of categories) {
        if (!first) await wait(city.crawlDelaySec * 1000);
        first = false;
        let res;
        try {
            res = await fetchImpl(feedUrl(city, catId), {
                headers: { "User-Agent": USER_AGENT, Accept: "text/calendar" },
                signal: AbortSignal.timeout(30000),
            });
        } catch (e) {
            res = { ok: false, status: e.message };
        }
        if (!res.ok) {
            console.warn(`⚠️ ${city.id} category ${catId}: HTTP ${res.status}`);
            complete = false;
            failures++;
            continue;
        }
        for (const e of parseEvents(await res.text())) {
            if (e.uid && !seen.has(e.uid)) {
                seen.add(e.uid);
                events.push({ ...e, category, categoryId: Number(catId) });
            }
        }
    }
    if (failures === categories.length) throw new Error(`${city.id}: every category feed failed`);
    return { events, complete };
}

/** Whether an activity scraped from `url` (by the Gemini jobs) is covered by this city's feed. */
function isSupersededUrl(url, city) {
    let host;
    try {
        host = new URL(url).host.toLowerCase().replace(/^www\./, "");
    } catch (e) {
        return false;
    }
    return host === new URL(city.baseUrl).host.replace(/^www\./, "");
}

module.exports = {
    CIVICPLUS_CITIES,
    USER_AGENT,
    classifyEvent,
    parseLocation,
    eventVenue,
    activityId,
    buildActivity,
    feedUrl,
    fetchAllEvents,
    isSupersededUrl,
};
