// The Events Calendar (TEC) adapter.
//
// TEC is a WordPress plugin used by many museums and parks. It publishes a public REST API
// (/wp-json/tribe/events/v1/events) that the plugin offers for other sites and apps to read.
// One adapter covers every site that runs it; adding a site is one entry in TEC_SITES.
// Each site's robots.txt crawl delay is respected, and every activity links back to the
// original event page.
//
// The API is structured (UTC and local times, all-day flag, categories, cost), so no LLM and
// no geocoding are needed. Most sites attach no venue to their events, so each site's
// coordinates are fixed in its config.

const { sessionDuration } = require("../utils/duration");

const USER_AGENT = "ToddleGoBot/0.1 (family events listing; links back to each event page)";

/**
 * Bay Area sites whose event calendars run on TEC.
 * - venue: fixed name, address and coordinates (looked up from the street address).
 * - crawlDelaySec: the site's robots.txt Crawl-delay, or a polite default.
 * - ageRange / ageRangeYears: the site's default audience when categories say nothing.
 * - costNote: shown when an event has no cost of its own; these programs come with admission.
 * - exclude: site-specific categories that are not activities for young children.
 */
const TEC_SITES = [
    {
        id: "fairyland",
        baseUrl: "https://fairyland.org",
        venue: {
            name: "Children's Fairyland",
            address: "699 Bellevue Ave, Oakland, CA 94610",
            lat: 37.8088234,
            lng: -122.2602497,
        },
        venueType: "park",
        isIndoor: false,
        crawlDelaySec: 10, // robots.txt: Crawl-delay: 10
        // A theme park for children under 8; puppet shows and storytimes are for little ones
        ageRange: "0-8 years",
        ageRangeYears: [0, 8],
        costNote: "Included with park admission",
        category: "Park Program",
    },
    {
        id: "chabot",
        baseUrl: "https://chabotspace.org",
        venue: {
            name: "Chabot Space & Science Center",
            address: "10000 Skyline Blvd, Oakland, CA 94619",
            lat: 37.8184631,
            lng: -122.1806867,
        },
        venueType: "museum",
        isIndoor: true,
        crawlDelaySec: 2,
        // Planetarium shows are for all ages, not made for toddlers
        ageRange: "All",
        ageRangeYears: null,
        costNote: "Included with museum admission",
        category: "Museum Program",
    },
];

// Calendar entries that are not activities: opening hours and closures
const NOT_AN_ACTIVITY_CATEGORY = /\bhours\b|\bclosed\b|closure/i;
const NOT_AN_ACTIVITY_TITLE = /^(open|closed)\b|\bclosed (for|on|early)\b|closure|admission hours/i;
// Not for young children
const GROWN_UP_CATEGORY = /adults?\b|\b(18|21)\s*\+|\bnight\b|teens?\b|members? only/i;
const GROWN_UP_TITLE = /\b(18|21)\s*\+|adults? only|\bsip\b|happy hour|\bteens?\b/i;
const CANCELLED_TITLE = /^(cancel+ed|postponed)\b/i;
// Categories that name a narrower age than the site default
const BABY_TODDLER = /\bbab(y|ies)\b|toddler|infant/i;
const PRESCHOOL = /pre-?school/i;
// Age notes at the start of descriptions, e.g. Chabot's "25 min Ages 6 and up, Adults"
const MIN_AGE_NOTE = /\bages?\s+(\d{1,2})\s*(?:\+|and\s+(?:up|over|older))/i;
const ALL_AGES_NOTE = /\ball ages\b/i;
const GROWN_UP_NOTE = /\b(?:adults?|ages?)\s+(?:only|(?:18|21)\s*(?:\+|and\s+(?:up|over)))|designed for adults/i;
// A show length at the start of a description, e.g. "25 min", "(15 min)", "35 minutes"
const SHOW_LENGTH_NOTE = /\(?\b(\d{1,3})\s*min(?:ute)?s?\b\)?/i;
/** How much of a description counts as its leading notes. */
const NOTE_CHARS = 100;

/** Youngest age ToddleGo is for is 0; programs whose minimum age is above this are left out. */
const MAX_MIN_AGE = 5;

/** Decodes the HTML entities WordPress puts in titles and descriptions ("&#038;", "&#8217;"). */
function decodeEntities(s) {
    return String(s || "")
        .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
        .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
        .replace(/&nbsp;/g, " ")
        .replace(/&quot;/g, "\"")
        .replace(/&apos;/g, "'")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&amp;/g, "&");
}

/** Plain-text excerpt of an HTML description, cut at a word boundary. */
function excerpt(html, maxLength = 280) {
    const plain = decodeEntities(String(html || "").replace(/<[^>]+>/g, " "))
        .replace(/\s+/g, " ")
        .trim();
    if (plain.length <= maxLength) return plain;
    return plain.slice(0, maxLength).replace(/\s+\S*$/, "") + "…";
}

/** "2026-10-14 18:00:00" (UTC, as TEC returns it) to Unix seconds. */
const utcToSec = (s) => Math.floor(Date.parse(`${String(s).replace(" ", "T")}Z`) / 1000);

/**
 * Decides whether an event suits young children, and the age range to show.
 * @param {string[]} categories - TEC category names.
 * @param {string} title - Decoded title.
 * @param {Object} site - Entry from TEC_SITES.
 */
function classifyEvent(categories, title, site, description = "") {
    const no = { include: false, ageRange: null, ageRangeYears: null };
    const excluded = site.exclude || [];
    if (categories.some((c) => NOT_AN_ACTIVITY_CATEGORY.test(c) || GROWN_UP_CATEGORY.test(c) || excluded.includes(c))) return no;
    if (NOT_AN_ACTIVITY_TITLE.test(title) || GROWN_UP_TITLE.test(title)) return no;
    const notes = description.slice(0, NOTE_CHARS);
    if (GROWN_UP_NOTE.test(notes)) return no;
    const minAge = notes.match(MIN_AGE_NOTE);
    if (minAge && Number(minAge[1]) > MAX_MIN_AGE) return no; // e.g. "Ages 6 and up"

    const text = [...categories, title];
    if (text.some((c) => BABY_TODDLER.test(c))) return { include: true, ageRange: "0-3 years", ageRangeYears: [0, 3] };
    if (text.some((c) => PRESCHOOL.test(c))) return { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] };
    if (ALL_AGES_NOTE.test(notes)) return { include: true, ageRange: "All", ageRangeYears: null };
    return { include: true, ageRange: site.ageRange, ageRangeYears: site.ageRangeYears };
}

/**
 * Length in minutes from a description's leading note ("25 min"), or null. Calendar slots
 * are often longer than the show itself (Chabot books an hour for a 25-minute show).
 */
function showLengthMinutes(description) {
    const m = String(description || "").slice(0, NOTE_CHARS).match(SHOW_LENGTH_NOTE);
    const minutes = m ? Number(m[1]) : NaN;
    return minutes > 0 && minutes <= 240 ? minutes : null;
}

/** Activity doc ID from the site's own event ID (each recurring session has its own). */
const activityId = (siteId, eventId) => `tec_${siteId}_${eventId}`;

/**
 * Maps one TEC event to an activity, before venue and series are linked (the sync job adds
 * venueId, coordinates, seriesId and type, like the other jobs).
 * @returns {null|{activityId: string, activity: Object, cancelled?: boolean}}
 *   null when the event is not for young children, hidden, or has no valid time;
 *   { cancelled: true } when the site marked it cancelled, so a saved copy can be removed.
 */
function buildActivity(site, event, { nowSec }) {
    const title = decodeEntities(event.title).trim();
    const id = activityId(site.id, event.id);
    if (CANCELLED_TITLE.test(title)) return { activityId: id, activity: null, cancelled: true };
    if (event.hide_from_listings || (event.status && event.status !== "publish")) return null;

    const categories = (event.categories || []).map((c) => decodeEntities(c.name).trim()).filter(Boolean);
    const description = excerpt(event.description) || excerpt(event.excerpt);
    const audience = classifyEvent(categories, title, site, description);
    if (!audience.include) return null;

    const startTime = utcToSec(event.utc_start_date);
    if (!Number.isFinite(startTime)) return null;
    // The stated show length wins over a longer calendar slot
    const listedEnd = utcToSec(event.utc_end_date);
    const showMinutes = event.all_day ? null : showLengthMinutes(description);
    const end = showMinutes && (!Number.isFinite(listedEnd) || startTime + showMinutes * 60 < listedEnd)
        ? startTime + showMinutes * 60
        : listedEnd;
    const { endTime, durationMinutes, durationEstimated } = sessionDuration(title, startTime, end);
    const cost = decodeEntities(event.cost).trim();

    const activity = {
        title,
        venue: site.venue.name,
        description: description || null,
        startTime,
        endTime,
        durationMinutes,
        durationEstimated,
        isAllDay: !!event.all_day,
        ageRange: audience.ageRange,
        age_range: audience.ageRangeYears,
        // Programs at these sites come with paid admission unless the event says it is free
        isFree: /^free\b/i.test(cost),
        cost: cost || site.costNote || null,
        requiresBooking: false,
        registrationUrl: null,
        locationDetails: event.venue && event.venue.venue ? decodeEntities(event.venue.venue) : null,
        isVirtual: !!event.is_virtual,
        isIndoor: site.isIndoor && !event.is_virtual,
        sourceUrl: event.url,
        source: "tec",
        sourceEventId: String(event.id),
        tenant: site.id,
        venueType: site.venueType,
        tags: categories,
        category: site.category,
        lastVerifiedAt: nowSec,
        // Remove from the app shortly after the event ends (Firestore TTL on expireAt)
        expireAt: new Date((endTime + 10 * 60) * 1000),
    };
    return { activityId: id, activity };
}

const dateParam = (sec) => new Date(sec * 1000).toISOString().slice(0, 10);

/**
 * Fetches every page of a site's events between two times, one page at a time with the
 * site's crawl delay between requests.
 * @returns {Promise<{events: Object[], complete: boolean}>} complete is false when the page
 *   cap stopped the read early, so callers do not treat unlisted events as removed.
 */
async function fetchAllEvents(site, { fromSec, toSec, perPage = 50, maxPages = 20, fetchImpl = fetch, sleep } = {}) {
    const wait = sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
    const events = [];
    const seen = new Set();
    for (let page = 1; page <= maxPages; page++) {
        if (page > 1) await wait(site.crawlDelaySec * 1000);
        const url = `${site.baseUrl}/wp-json/tribe/events/v1/events?start_date=${dateParam(fromSec)}` +
            `&end_date=${dateParam(toSec)}&per_page=${perPage}&page=${page}`;
        const res = await fetchImpl(url, {
            headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
            signal: AbortSignal.timeout(30000),
        });
        // TEC answers a page past the last one with 400 "rest_invalid_param"
        if (res.status === 400 && page > 1) return { events, complete: true };
        if (!res.ok) throw new Error(`${site.id} events page ${page}: HTTP ${res.status}`);
        const body = await res.json();
        for (const e of body.events || []) {
            if (e && e.id && !seen.has(e.id)) {
                seen.add(e.id);
                events.push(e);
            }
        }
        if (!body.total_pages || page >= body.total_pages) return { events, complete: true };
    }
    return { events, complete: false };
}

/** Whether an activity scraped from `url` (by the Gemini jobs) is covered by this site's API. */
function isSupersededUrl(url, site) {
    let host;
    try {
        host = new URL(url).host.toLowerCase().replace(/^www\./, "");
    } catch (e) {
        return false;
    }
    return host === new URL(site.baseUrl).host.replace(/^www\./, "");
}

module.exports = {
    TEC_SITES,
    USER_AGENT,
    decodeEntities,
    excerpt,
    classifyEvent,
    showLengthMinutes,
    activityId,
    buildActivity,
    fetchAllEvents,
    isSupersededUrl,
};
