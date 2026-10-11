// BiblioCommons events adapter.
//
// Reads each library's public events RSS feed, the channel BiblioCommons' terms
// explicitly permit for automated access ("except as may be specifically permitted
// using RSS/XML feeds"). The gateway JSON API and HTML pages are NOT used at runtime.
// Every activity keeps a link to the original event page, as the terms require for extracts.
//
// The feed is structured (local/UTC times, audience categories, branch address with
// coordinates, cancellation flag), so no LLM and no geocoding are needed.

const xml2js = require("xml2js");

const GATEWAY = "https://gateway.bibliocommons.com/v2/libraries";
const USER_AGENT = "ToddleGoBot/0.1 (family events listing; links back to each event page)";

/**
 * Bay Area libraries whose events are hosted on BiblioCommons.
 * audienceIds: the library's own audience IDs for babies/toddlers/preschoolers/families, so
 * the feed only returns those events (IDs differ per library). Generated with
 * `node discover-bibliocommons-audiences.js <tenant>`; without them the full feed is read
 * and filtered by category name, which takes many more requests.
 * websiteHosts: the library's own website. Activities other jobs scraped with Gemini from
 * these hosts (or from the BiblioCommons site) are superseded by this feed and removed.
 */
const BAY_AREA_TENANTS = [
    // Babies & Toddlers | Preschoolers
    { tenant: "aclibrary", audienceIds: ["5dbcad183994473a00ac6e6a", "5dbcad1f3994473a00ac6e6b"], websiteHosts: ["aclibrary.org"] },
    // Young Children, ages 0-5
    { tenant: "sjpl", audienceIds: ["5d5f09306bb98139001cffcc"], websiteHosts: ["sjpl.org", "sjlibrary.org"] },
    // Birth to 5 | Toddlers | Families | Preschoolers | Babies
    { tenant: "oaklandlibrary", audienceIds: ["60b6b3673fb00b370086120f", "60b6b397bf42783000f0fb2b", "6101cafe8d4f1f31003e6ef2", "60b6b39e39528429008b3ae7", "60b6b382db93b64200b4749d"], websiteHosts: ["oaklandlibrary.org"] },
    // Babies (Birth to 1 year) | Toddlers (1 to 3 years) | Preschoolers (3-5 years)
    { tenant: "ccclib", audienceIds: ["5d1b98fd4538d8125347c6b8", "5d1b98f7b382898c51d88eec", "5d97e5b467238d2f006ebd5d"], websiteHosts: ["ccclib.org"] },
    // Kids: Family Events | Kids: Babies | Kids: Toddlers | Kids: Preschoolers
    { tenant: "sccl", audienceIds: ["5b2a5dcb2c1d736b168c62ac", "5b28181c4727c7344c796675", "5b28181c4727c7344c796676", "5b28181c4727c7344c796677"], websiteHosts: ["sccld.org"] },
    // Pre-schoolers (3-5) | Toddlers (18 mos. to 3 yrs) | Babies (under 2)
    { tenant: "paloalto", audienceIds: ["59a6e0705e7f62711a36e6ac", "59a6e0705e7f62711a36e6ad", "59a6e0705e7f62711a36e6ae"], websiteHosts: ["library.cityofpaloalto.org"] },
    // Families | Babies (0-1 yrs) | Toddlers (1-3 yrs) | Preschoolers (3-5 yrs)
    { tenant: "marinlibrary", audienceIds: ["5fc70596c6979286093e62a3", "5fc52113ebec963a0084eb4b", "5ff7aeb025c2fc430bf25de4", "5ff7aec25b24473a006c31fc"], websiteHosts: ["marinlibrary.org"] },
    // Preschoolers (0-5)
    { tenant: "smcl", audienceIds: ["564274cf4d0090f742000011"], websiteHosts: ["smcl.org"] },
];

// Audience labels vary by library: "Babies & Toddlers", "Kids: Toddlers", "Birth to 5",
// "Young Children, ages 0-5", "Pre-schoolers (3-5)", "Preschoolers (0-5)", "Families", ...
const BABY_TODDLER = /\bbab(y|ies)\b|toddler|infant/i;
const PRESCHOOL = /pre-?school|young children|birth to [1-5]\b|\b0\s*-\s*5\b/i;
const ZERO_TO_FIVE = /birth to 5\b|\b0\s*-\s*5\b/i;
const FAMILY = /famil/i;
const FAMILY_OR_STORYTIME = /famil|storytime|story time|everyone|all ages/i;
// Not activities for young children, whatever their audience tags say: closure notices,
// free meal/snack services for all children ("Snacks and Meals", "Meals at the Library"),
// and school-age academics ("Math Program for K-3rd", "Homework Help"). Category labels
// like "Academic Support & Homework Help" are not used: some libraries put storytimes there.
const NOT_AN_ACTIVITY_CATEGORY = /closure|holiday|closed|\bmeals?\b/i;
const NOT_AN_ACTIVITY_TITLE = /\bmeals?\b|free snacks|snack break|homework|tutor|\bK\s?-\s?\d/i;
// School-age programs libraries also file under "Families"; only excluded when the event
// has no explicit baby/toddler/preschool audience.
const SCHOOL_AGE_TITLE = /homework|after[- ]?school|tutor|\btweens?\b|\bteens?\b/i;

/** Whether an audience label should be requested from the feed: children under 5 or families. */
function isYoungChildAudience(name) {
    return BABY_TODDLER.test(name) || PRESCHOOL.test(name) || FAMILY.test(name);
}

/**
 * Decides whether an event suits children 0-5 from its categories, and the age range to show.
 * @param {string[]} categories - The item's <category> values (audiences, types, languages).
 * @param {string} [title] - Used to drop school-age programs tagged only as family events.
 * @returns {{include: boolean, ageRange: string, ageRangeYears: number[]|null}}
 */
function classifyAudience(categories, title = "") {
    if (categories.some((c) => NOT_AN_ACTIVITY_CATEGORY.test(c)) || NOT_AN_ACTIVITY_TITLE.test(title)) {
        return { include: false, ageRange: null, ageRangeYears: null };
    }
    const baby = categories.some((c) => BABY_TODDLER.test(c));
    const preschool = categories.some((c) => PRESCHOOL.test(c));
    if (baby && preschool) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (baby) return { include: true, ageRange: "0-3 years", ageRangeYears: [0, 3] };
    if (preschool) {
        // "Preschoolers" alone means 3-5; labels like "Young Children, ages 0-5" cover babies too
        const zeroToFive = categories.some((c) => ZERO_TO_FIVE.test(c));
        return zeroToFive
            ? { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] }
            : { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] };
    }
    if (categories.some((c) => FAMILY_OR_STORYTIME.test(c)) && !SCHOOL_AGE_TITLE.test(title)) {
        return { include: true, ageRange: "All", ageRangeYears: null };
    }
    return { include: false, ageRange: null, ageRangeYears: null };
}

const text = (v) => {
    if (v === undefined || v === null) return "";
    if (Array.isArray(v)) return text(v[0]);
    if (typeof v === "object") return text(v._);
    return String(v).trim();
};

/** Plain-text excerpt of an HTML description (the terms allow extracts that link back). */
function excerpt(html, maxLength = 280) {
    const plain = String(html || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&#39;|&apos;/g, "'")
        .replace(/&quot;/g, "\"")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/\s+/g, " ")
        .trim();
    if (plain.length <= maxLength) return plain;
    return plain.slice(0, maxLength).replace(/\s+\S*$/, "") + "…";
}

/**
 * Parses one page of a BiblioCommons events RSS feed.
 * @returns {Promise<{libraryName: string, items: Object[]}>}
 */
async function parseFeed(xml) {
    const parsed = await xml2js.parseStringPromise(xml, { explicitArray: true, trim: true });
    const channel = parsed?.rss?.channel?.[0] || {};
    // Channel title is "Events | <Library name>"
    const libraryName = text(channel.title).replace(/^Events\s*\|\s*/i, "");
    const items = (channel.item || []).map((it) => {
        const loc = it["bc:location"]?.[0] || {};
        const reg = it["bc:registration_info"]?.[0] || {};
        const link = text(it.link);
        return {
            id: link.split("/").pop(),
            title: text(it.title),
            descriptionHtml: text(it.description),
            link,
            categories: (it.category || []).map(text).filter(Boolean),
            startUtc: text(it["bc:start_date"]),
            endUtc: text(it["bc:end_date"]),
            startLocal: text(it["bc:start_date_local"]),
            isCancelled: text(it["bc:is_cancelled"]) === "true",
            isVirtual: text(it["bc:is_virtual"]) === "true",
            location: {
                id: text(loc["bc:id"]),
                name: text(loc["bc:name"]),
                number: text(loc["bc:number"]),
                street: text(loc["bc:street"]),
                city: text(loc["bc:city"]),
                zip: text(loc["bc:zip"]),
                state: text(loc["bc:state"]),
                latitude: Number(text(loc["bc:latitude"])),
                longitude: Number(text(loc["bc:longitude"])),
                details: text(loc["bc:location_details"]),
            },
            registration: {
                required: text(reg["bc:is_required"]) === "true",
                full: text(reg["bc:is_full"]) === "true",
            },
        };
    });
    return { libraryName, items };
}

/** Activity doc ID from the platform's own event ID, so it can never collide. */
const activityId = (tenant, eventId) => `bc_${tenant}_${eventId}`;

/** Display name for a branch, e.g. "Alameda County Library - Fremont". */
function venueName(libraryName, location) {
    if (!location.name) return libraryName;
    if (libraryName.toLowerCase().includes(location.name.toLowerCase())) return libraryName;
    return `${libraryName} - ${location.name}`;
}

function hasCoordinates(location) {
    return Number.isFinite(location.latitude) && Number.isFinite(location.longitude) &&
        !(location.latitude === 0 && location.longitude === 0);
}

/** One-line street address, e.g. "2400 Stevenson Blvd, Fremont, CA 94538". */
function formatAddress({ number, street, city, state, zip }) {
    const line = [number, street].filter(Boolean).join(" ");
    const region = [state, zip].filter(Boolean).join(" ");
    return [line, city, region].filter(Boolean).join(", ") || null;
}

/**
 * Whether an activity scraped from `url` is covered by this library's feed: the URL is on
 * the library's BiblioCommons site or its own website.
 */
function isSupersededUrl(url, { tenant, websiteHosts = [] }) {
    let host;
    try {
        host = new URL(url).host.toLowerCase().replace(/^www\./, "");
    } catch (e) {
        return false;
    }
    if (host === `${tenant}.bibliocommons.com`) return true;
    return websiteHosts.some((h) => host === h || host.endsWith(`.${h}`));
}

/**
 * Maps one feed item to an activity, before venue and series are linked.
 * Location fields (venueId, coordinates, geohash), seriesId, type and timing.recurrence
 * are added by the sync job via utils/venues and utils/series, like the other jobs.
 * @returns {null|{activityId: string, activity: Object, venue: {name, lat, lng, address}}}
 *   null when the item is not for young children, cancelled, or has no coordinates.
 */
function buildActivity(tenant, libraryName, item, { nowSec }) {
    const audience = classifyAudience(item.categories, item.title);
    if (!audience.include || item.isCancelled || !hasCoordinates(item.location)) return null;

    const startTime = Math.floor(Date.parse(item.startUtc) / 1000);
    const parsedEnd = Math.floor(Date.parse(item.endUtc) / 1000);
    if (!Number.isFinite(startTime)) return null;
    const endTime = Number.isFinite(parsedEnd) && parsedEnd > startTime ? parsedEnd : startTime + 60 * 60;
    const name = venueName(libraryName, item.location);
    // Libraries enter all-day events (displays, drop-in activities) as 00:00-23:59 local
    const isAllDay = item.startLocal.endsWith("T00:00") && endTime - startTime >= 23 * 60 * 60;

    const activity = {
        title: item.title,
        venue: name,
        description: excerpt(item.descriptionHtml) || null,
        startTime,
        endTime,
        isAllDay,
        ageRange: audience.ageRange,
        age_range: audience.ageRangeYears,
        isFree: true, // public library programs
        requiresBooking: item.registration.required,
        isFull: item.registration.full,
        registrationUrl: item.registration.required ? item.link : null,
        locationDetails: item.location.details || null,
        isVirtual: item.isVirtual,
        isIndoor: !item.isVirtual,
        sourceUrl: item.link,
        source: "bibliocommons",
        sourceEventId: item.id,
        tenant,
        tags: item.categories,
        category: "Library Program",
        lastVerifiedAt: nowSec,
        // Remove from the app shortly after the event ends (Firestore TTL on expireAt)
        expireAt: new Date((endTime + 10 * 60) * 1000),
    };
    const venue = {
        name,
        lat: item.location.latitude,
        lng: item.location.longitude,
        address: formatAddress(item.location),
    };
    return { activityId: activityId(tenant, item.id), activity, venue };
}

/**
 * Fetches every page of a library's events feed (optionally filtered to audienceIds).
 * Pages are requested one at a time with a delay, and stop at the first empty page.
 */
async function fetchAllItems(tenant, { audienceIds = [], delayMs = 1500, maxPages = 80, fetchImpl = fetch } = {}) {
    const items = [];
    const seen = new Set();
    let libraryName = "";
    let complete = false; // reached the end of the feed (not the page cap)
    const audienceParam = audienceIds.length ? `&audiences=${audienceIds.join(",")}` : "";
    for (let page = 1; page <= maxPages; page++) {
        if (page > 1) await new Promise((r) => setTimeout(r, delayMs));
        const res = await fetchImpl(`${GATEWAY}/${tenant}/rss/events?page=${page}${audienceParam}`, {
            headers: { "User-Agent": USER_AGENT },
            signal: AbortSignal.timeout(30000),
        });
        if (!res.ok) throw new Error(`${tenant} feed page ${page}: HTTP ${res.status}`);
        const feed = await parseFeed(await res.text());
        libraryName = libraryName || feed.libraryName;
        const fresh = feed.items.filter((it) => it.id && !seen.has(it.id));
        if (fresh.length === 0) {
            complete = true; // past the last page (or a repeated page)
            break;
        }
        fresh.forEach((it) => seen.add(it.id));
        items.push(...fresh);
    }
    return { libraryName, items, complete };
}

module.exports = {
    BAY_AREA_TENANTS,
    USER_AGENT,
    isYoungChildAudience,
    classifyAudience,
    excerpt,
    parseFeed,
    activityId,
    venueName,
    hasCoordinates,
    formatAddress,
    isSupersededUrl,
    buildActivity,
    fetchAllItems,
};
