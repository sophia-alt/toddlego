// Offline tests for the Communico adapter (sources/communico.js) and the shared library
// audience rules (utils/libraryAudience.js). Synthetic events and a fake fetch; no network,
// Firestore or paid APIs.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const communico = require("../sources/communico");
const { classifyEvent } = require("../utils/libraryAudience");
const { partition } = require("../utils/sourceSync");

const EVENTS = JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", "communico-events.json"), "utf8"));
const NOW = Math.floor(Date.parse("2026-10-13T00:00:00Z") / 1000);
const LIBRARY = {
    tenant: "examplelib",
    websiteHosts: ["examplelibrary.org"],
    branches: [
        { locationIds: ["10"], match: /central/i, name: "Example Library - Central", address: "1 Main St, Example, CA", lat: 37.87, lng: -122.27 },
        { locationIds: ["11"], match: /south/i, name: "Example Library - South Branch", address: "2 Russell St, Example, CA", lat: 37.86, lng: -122.27 },
    ],
    places: [
        { match: /the freight/i, name: "Freight & Salvage", address: "2020 Addison St, Example, CA", lat: 37.871, lng: -122.27, venueType: "music_venue" },
        { match: /ywca/i, name: "YWCA Downtown", address: "3 Bancroft Way, Example, CA", lat: 37.869, lng: -122.257 },
    ],
};
const build = (id) => communico.buildActivity(LIBRARY, EVENTS.find((e) => e.id === String(id)), { nowSec: NOW });

test("maps a storytime at its branch, with Pacific times", () => {
    const { activityId, activity, venue } = build(301);
    assert.strictEqual(activityId, "communico_examplelib_301");
    assert.strictEqual(activity.title, "Baby/Toddler Storytime @South");
    assert.strictEqual(activity.venue, "Example Library - South Branch");
    assert.strictEqual(activity.startTime, Date.parse("2026-10-14T17:30:00Z") / 1000);
    assert.strictEqual(activity.durationMinutes, 30);
    assert.strictEqual(activity.ageRange, "0-5 years", "Early Childhood");
    assert.strictEqual(activity.description, "Stories, songs & rhymes for little ones.");
    assert.strictEqual(activity.isFree, true);
    assert.strictEqual(activity.requiresBooking, false);
    assert.strictEqual(activity.isIndoor, true);
    assert.strictEqual(activity.source, "communico");
    assert.strictEqual(activity.sourceUrl, "https://examplelib.libnet.info/event/301");
    assert.deepStrictEqual(venue, { name: "Example Library - South Branch", address: "2 Russell St, Example, CA", lat: 37.86, lng: -122.27 });
});

test("off-site places: listed ones get their coordinates, others are skipped", () => {
    const freight = build(302).activity;
    assert.strictEqual(freight.venue, "Freight & Salvage");
    assert.strictEqual(freight.locationDetails, "Main Music Hall");
    assert.strictEqual(freight.venueType, "music_venue");
    assert.strictEqual(freight.isIndoor, false, "only library branches are known to be indoors");
    assert.strictEqual(freight.registrationUrl, "https://tickets.example.org/storytime", "third-party registration");
    assert.strictEqual(freight.description, null);

    // Carries the Central branch's location ID with another place's name
    const ywca = build(303).activity;
    assert.strictEqual(ywca.venue, "YWCA Downtown");
    assert.strictEqual(ywca.venueType, "community_center");
    assert.strictEqual(ywca.registrationUrl, "https://examplelib.libnet.info/event/303");

    assert.strictEqual(build(307), null, "unlisted off-site place");
});

test("online events use the first branch and keep their fee", () => {
    const online = build(308).activity;
    assert.strictEqual(online.venue, "Example Library - Central");
    assert.strictEqual(online.isVirtual, true);
    assert.strictEqual(online.isIndoor, false);
    assert.strictEqual(online.ageRange, "0-3 years");
    assert.strictEqual(online.isFree, false);
    assert.strictEqual(online.cost, "$5");
    assert.strictEqual(online.startTime, Date.parse("2026-11-05T18:00:00Z") / 1000, "PST");
});

test("events tagged for every age group and changed hours are left out", () => {
    assert.strictEqual(build(304), null, "flu clinic tagged Early Childhood through Adults");
    assert.strictEqual(build(305), null, "late opening");
});

test("cancelled and rescheduled sessions are reported so a saved copy can be removed", () => {
    assert.deepStrictEqual(build(306), { activityId: "communico_examplelib_306", activity: null, cancelled: true });
    const moved = { ...EVENTS[0], id: "309", changed: "3" };
    assert.strictEqual(communico.buildActivity(LIBRARY, moved, { nowSec: NOW }).cancelled, true);
    const newSession = { ...EVENTS[0], id: "310", changed: "0" };
    assert.ok(communico.buildActivity(LIBRARY, newSession, { nowSec: NOW }).activity, "the moved-to session is kept");
    const privateEvent = { ...EVENTS[0], id: "311", private_event: "1" };
    assert.strictEqual(communico.buildActivity(LIBRARY, privateEvent, { nowSec: NOW }), null);
});

test("shared audience rules: all-ages tagging and grown-up audiences", () => {
    const everyone = ["Teens", "Preteens", "Elementary", "Early Childhood", "Adults"];
    assert.strictEqual(classifyEvent(everyone, ["Storytime"], "BPL Comic Con!").include, false);
    assert.strictEqual(classifyEvent(everyone, [], "Baby Bounce").ageRange, "0-3 years", "title still counts");
    assert.strictEqual(classifyEvent(["Elementary", "Early Childhood"], [], "Kids Yoga").ageRange, "0-5 years");
    assert.strictEqual(classifyEvent(["School age", "Infants & Toddlers"], [], "Mosaic Monday").ageRange, "0-3 years");
    assert.strictEqual(classifyEvent(["Elementary"], ["Storytime"], "Chapter Book Club").include, false);
    assert.strictEqual(classifyEvent(["Families", "Adults"], [], "Family Dance Party").ageRange, "All");
});

test("fetchAllEvents asks for 60 days from today (Pacific) in one request", async () => {
    const urls = [];
    const fetchImpl = async (url, opts) => {
        urls.push(url);
        assert.match(opts.headers["User-Agent"], /^ToddleGoBot/);
        return { ok: true, status: 200, text: async () => JSON.stringify([...EVENTS, EVENTS[0]]) };
    };
    // 2026-10-13 06:00 UTC is still 10/12 in California
    const nowSec = Date.parse("2026-10-13T06:00:00Z") / 1000;
    const { events, complete } = await communico.fetchAllEvents(LIBRARY, { nowSec, fetchImpl });
    assert.strictEqual(events.length, EVENTS.length, "duplicate IDs dropped");
    assert.strictEqual(complete, true);
    assert.strictEqual(urls.length, 1);
    const url = new URL(urls[0]);
    assert.strictEqual(url.host, "examplelib.libnet.info");
    assert.strictEqual(url.pathname, "/eeventcaldata");
    const req = JSON.parse(url.searchParams.get("req"));
    assert.strictEqual(req.date, "2026-10-12");
    assert.strictEqual(req.days, 60);
});

test("fetchAllEvents fails clearly on errors and maintenance pages", async () => {
    const denied = async () => ({ ok: false, status: 403 });
    await assert.rejects(communico.fetchAllEvents(LIBRARY, { fetchImpl: denied }), /examplelib events: HTTP 403/);
    const maintenance = async () => ({ ok: true, status: 200, text: async () => "<html><title>Site Maintenance</title></html>" });
    await assert.rejects(communico.fetchAllEvents(LIBRARY, { fetchImpl: maintenance }), /examplelib events: not JSON/);
});

test("Gemini-scraped activities from the library's sites are superseded", () => {
    assert.strictEqual(communico.isSupersededUrl("https://examplelib.libnet.info/event/1", LIBRARY), true);
    assert.strictEqual(communico.isSupersededUrl("https://www.examplelibrary.org/events", LIBRARY), true);
    assert.strictEqual(communico.isSupersededUrl("https://otherlib.libnet.info/event/1", LIBRARY), false);
    assert.strictEqual(communico.isSupersededUrl("not a url", LIBRARY), false);
});

test("partition keeps the next 60 days and collects cancellations", () => {
    const at = (startTime) => ({ activityId: `a${startTime}`, activity: { startTime, endTime: startTime + 1800 } });
    const { built, cancelledIds, skipped } = partition([
        at(NOW + 3600), at(NOW - 7200), at(NOW + 61 * 86400), null,
        { activityId: "gone", activity: null, cancelled: true },
    ], NOW);
    assert.deepStrictEqual(built.map((b) => b.activityId), [`a${NOW + 3600}`]);
    assert.deepStrictEqual(cancelledIds, ["gone"]);
    assert.strictEqual(skipped, 3);
});

test("every configured library has branches with IDs and valid coordinates", () => {
    for (const library of communico.COMMUNICO_LIBRARIES) {
        assert.ok(library.branches.length > 0, library.tenant);
        for (const b of library.branches) assert.ok(b.locationIds.length > 0 && b.match, `${b.name} is matchable`);
        for (const p of [...library.branches, ...library.places]) {
            assert.ok(p.lat > 36 && p.lat < 39 && p.lng > -123.5 && p.lng < -121, `${p.name} is in the Bay Area`);
            assert.ok(p.address, `${p.name} has an address`);
        }
    }
});
