// Offline tests for the LibCal adapter (sources/libcal.js).
// Uses synthetic API pages and a fake fetch; no network, Firestore or paid APIs.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const libcal = require("../sources/libcal");

const page = (n) => JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", `libcal-events-page${n}.json`), "utf8"));
const NOW = Math.floor(Date.parse("2026-10-13T00:00:00Z") / 1000);
const LIBRARY = {
    tenant: "examplelib",
    calendarIds: [100],
    websiteHosts: ["examplelibrary.org"],
    websitePaths: ["examplecity.gov/library"],
    branches: [
        { name: "Example Library", address: "1 Main St, Example, CA", lat: 37.39, lng: -122.08 },
        { match: /easton/i, name: "Example Library - Easton Branch", address: "2 Easton Dr, Example, CA", lat: 37.58, lng: -122.37 },
    ],
    places: [
        { match: /pioneer park/i, name: "Pioneer Park, Example", address: "Church St, Example, CA", lat: 37.389, lng: -122.083, venueType: "park" },
    ],
};
const allEvents = () => [...page(1).results, ...page(2).results];
const build = (id) => libcal.buildActivity(LIBRARY, allEvents().find((e) => e.id === id), { nowSec: NOW });
const cls = (audiences, categories, title) => libcal.classifyEvent(audiences, categories, title);

test("converts LibCal's Pacific local times across daylight saving", () => {
    assert.strictEqual(libcal.pacificToSec("2026-10-14 10:30:00"), Date.parse("2026-10-14T17:30:00Z") / 1000, "PDT");
    assert.strictEqual(libcal.pacificToSec("2026-11-26 10:30:00"), Date.parse("2026-11-26T18:30:00Z") / 1000, "PST");
    assert.strictEqual(libcal.pacificToSec("2026-11-01 01:30:00") % 60, 0, "ambiguous hour still resolves");
    assert.ok(Number.isNaN(libcal.pacificToSec("")));
});

test("maps a baby storytime at the main branch", () => {
    const { activityId, activity, venue } = build(201);
    assert.strictEqual(activityId, "libcal_examplelib_201");
    assert.strictEqual(activity.title, "Baby Storytime");
    assert.strictEqual(activity.venue, "Example Library");
    assert.strictEqual(activity.locationDetails, "1st Floor Program Room");
    assert.strictEqual(activity.startTime, Date.parse("2026-10-14T17:30:00Z") / 1000);
    assert.strictEqual(activity.durationMinutes, 30);
    assert.strictEqual(activity.ageRange, "0-3 years");
    assert.strictEqual(activity.description, "Songs & rhymes for babies.", "style block and comment dropped");
    assert.strictEqual(activity.isFree, true);
    assert.strictEqual(activity.requiresBooking, false);
    assert.strictEqual(activity.isIndoor, true);
    assert.strictEqual(activity.venueType, "library");
    assert.strictEqual(activity.source, "libcal");
    assert.strictEqual(activity.sourceUrl, "https://examplelib.libcal.com/event/201");
    assert.deepStrictEqual(venue, { name: "Example Library", address: "1 Main St, Example, CA", lat: 37.39, lng: -122.08 });
});

test("park storytimes use the park's coordinates; branches come from campus or location", () => {
    const park = build(202);
    assert.strictEqual(park.activity.venue, "Pioneer Park, Example");
    assert.strictEqual(park.activity.locationDetails, null, "location only repeats the park");
    assert.strictEqual(park.activity.isIndoor, false);
    assert.strictEqual(park.activity.venueType, "park");
    assert.strictEqual(park.activity.ageRange, "0-5 years");
    assert.strictEqual(park.activity.description, "Stories in the park.", "falls back to shortdesc");
    assert.strictEqual(park.venue.lat, 37.389);

    const easton = build(203).activity;
    assert.strictEqual(easton.venue, "Example Library - Easton Branch");
    assert.strictEqual(easton.locationDetails, null);
    assert.strictEqual(easton.ageRange, "2-5 years", "age span in the title");
    assert.strictEqual(easton.requiresBooking, true);
    assert.strictEqual(easton.registrationUrl, "https://examplelib.libcal.com/event/203");
});

test("an off-site event at an unlisted place is skipped, not shown at the library", () => {
    const event = { ...page(1).results[0], id: 299, title: "Cuesta Park Storytime", location: "Offsite" };
    assert.strictEqual(libcal.buildActivity(LIBRARY, event, { nowSec: NOW }), null);
    assert.strictEqual(libcal.placeFor(LIBRARY, { ...event, location: "Some Other Park" }), null);
    assert.strictEqual(libcal.placeFor(LIBRARY, { ...event, location: "Outdoor Amphitheater" }).name, "Example Library");
});

test("drops closures and school-age programs; cancelled sessions are reported", () => {
    assert.strictEqual(build(204), null, "holiday closure");
    assert.strictEqual(build(205), null, "Ages 5-11 for Grades 1-6");
    assert.deepStrictEqual(build(206), { activityId: "libcal_examplelib_206", activity: null, cancelled: true });
});

test("audience rules", () => {
    assert.strictEqual(cls(["Toddler (19 mos-3 yrs)"], ["Storytime"], "Move and Groove").ageRange, "0-3 years");
    assert.strictEqual(cls(["Preschoolers", "Parents", "Toddlers"], [], "Smart Start Babytime").ageRange, "0-5 years");
    assert.strictEqual(cls(["Children"], ["Storytime"], "Sing & Stomp").ageRange, "0-5 years");
    assert.strictEqual(cls([], [], "Holiday Storytime").include, true, "a holiday-themed storytime is not a closure");
    assert.strictEqual(cls(["Families"], [], "Family Movie").ageRange, "All");
    assert.strictEqual(cls(["Children"], ["Education"], "Full STEAM Ahead").include, false, "\"Children\" alone");
    assert.strictEqual(cls(["Children", "Middle School"], [], "Tween LEGO Club (Ages 9-13)").include, false);
    assert.strictEqual(cls(["Adults"], ["Storytime"], "Storytime for Grown-ups").include, false);
    assert.strictEqual(cls([], ["Storytime"], "CLOSED: Veterans Day").include, false);
    assert.strictEqual(cls([], [], "Day Before Thanksgiving: Libraries Close @ 5PM").include, false);
});

test("fetchAllEvents pages with LibCal's crawl delay and keeps configured calendars", async () => {
    const urls = [];
    const waits = [];
    const fetchImpl = async (url, opts) => {
        urls.push(url);
        assert.match(opts.headers["User-Agent"], /^ToddleGoBot/);
        const n = Number(new URL(url).searchParams.get("page"));
        return { ok: true, status: 200, json: async () => page(n) };
    };
    const { events, complete } = await libcal.fetchAllEvents(LIBRARY, {
        perPage: 4, fetchImpl, sleep: async (ms) => waits.push(ms),
    });
    assert.deepStrictEqual(events.map((e) => e.id), [201, 202, 203, 204, 205, 206], "bookmobile calendar skipped");
    assert.strictEqual(complete, true);
    assert.strictEqual(urls.length, 2);
    assert.match(urls[0], /^https:\/\/examplelib\.libcal\.com\/ajax\/calendar\/list\?c=-1&date=0000-00-00&perpage=4&page=1&/);
    assert.deepStrictEqual(waits, [10000], "10 s between pages");
});

test("fetchAllEvents reports an incomplete read at the page cap, and HTTP errors", async () => {
    const full = async () => ({ ok: true, status: 200, json: async () => page(1) });
    const { complete } = await libcal.fetchAllEvents(LIBRARY, { perPage: 4, maxPages: 1, fetchImpl: full, sleep: async () => {} });
    assert.strictEqual(complete, false);
    const denied = async () => ({ ok: false, status: 403 });
    await assert.rejects(libcal.fetchAllEvents(LIBRARY, { fetchImpl: denied }), /examplelib events page 1: HTTP 403/);
});

test("Gemini-scraped activities from the library's sites are superseded, not the city's", () => {
    assert.strictEqual(libcal.isSupersededUrl("https://examplelib.libcal.com/event/1", LIBRARY), true);
    assert.strictEqual(libcal.isSupersededUrl("https://www.examplelibrary.org/events", LIBRARY), true);
    assert.strictEqual(libcal.isSupersededUrl("https://www.examplecity.gov/library/kids", LIBRARY), true);
    assert.strictEqual(libcal.isSupersededUrl("https://examplecity.gov/library", LIBRARY), true);
    assert.strictEqual(libcal.isSupersededUrl("https://examplecity.gov/parks/events", LIBRARY), false, "city's other events kept");
    assert.strictEqual(libcal.isSupersededUrl("https://examplecity.gov/library-board", LIBRARY), false);
    assert.strictEqual(libcal.isSupersededUrl("not a url", LIBRARY), false);
});

test("every configured library has a default branch with valid coordinates", () => {
    for (const library of libcal.LIBCAL_LIBRARIES) {
        assert.ok(library.calendarIds.length > 0, library.tenant);
        assert.ok(!library.branches[0].match, `${library.tenant}: first branch is the default`);
        for (const p of [...library.branches, ...library.places]) {
            assert.ok(p.lat > 36 && p.lat < 39 && p.lng > -123.5 && p.lng < -121, `${p.name} is in the Bay Area`);
            assert.ok(p.address, `${p.name} has an address`);
        }
    }
});
