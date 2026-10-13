// Offline tests for The Events Calendar adapter (sources/tec.js).
// Uses synthetic API pages and a fake fetch; no network, Firestore or paid APIs.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const tec = require("../sources/tec");

const page = (n) => JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", `tec-events-page${n}.json`), "utf8"));
const NOW = Math.floor(Date.parse("2026-10-13T00:00:00Z") / 1000);
const SITE = {
    id: "examplepark",
    baseUrl: "https://example-park.org",
    venue: { name: "Example Park", address: "1 Main St, Oakland, CA", lat: 37.8, lng: -122.26 },
    venueType: "park",
    isIndoor: false,
    crawlDelaySec: 10,
    ageRange: "0-8 years",
    ageRangeYears: [0, 8],
    costNote: "Included with park admission",
    category: "Park Program",
};
const allEvents = () => [...page(1).events, ...page(2).events];
const build = (id) => tec.buildActivity(SITE, allEvents().find((e) => e.id === id), { nowSec: NOW });

test("decodes WordPress entities and strips HTML", () => {
    assert.strictEqual(tec.decodeEntities("Paint &#038; Sip"), "Paint & Sip");
    assert.strictEqual(tec.decodeEntities("Earth&#8217;s moon"), "Earth’s moon");
    assert.strictEqual(tec.excerpt("<p>Playing at</p><p>the theater &#8211; 11 a.m.</p>"), "Playing at the theater – 11 a.m.");
});

test("drops opening hours, adult events, and shows for ages 6 and up", () => {
    assert.strictEqual(build(101), null, "park hours");
    assert.strictEqual(build(104), null, "Adults 18+");
    assert.strictEqual(build(105), null, "Ages 6 and up");
    const cls = (cats, title, desc) => tec.classifyEvent(cats, title, SITE, desc).include;
    assert.strictEqual(cls(["Special Hours"], "Halloween General Admission Hours"), false);
    assert.strictEqual(cls(["Special Event"], "CuriOdyssey will be closed for maintenance."), false);
    assert.strictEqual(cls(["Hikes"], "Hike & Sip", "Nature, History, and Stargazing Designed for adults 21 and over."), false);
    assert.strictEqual(cls(["Planetarium"], "Starry Night", "20 min Ages 3 and up"), true);
});

test("maps a puppet show with the site's venue, audience and admission note", () => {
    const { activityId, activity } = build(102);
    assert.strictEqual(activityId, "tec_examplepark_102");
    assert.strictEqual(activity.title, "Puppet Show: The Little Red Hen");
    assert.strictEqual(activity.venue, "Example Park");
    assert.strictEqual(activity.locationDetails, "Storybook Theater");
    assert.strictEqual(activity.startTime, Math.floor(Date.parse("2026-10-14T18:00:00Z") / 1000));
    assert.strictEqual(activity.durationMinutes, 30);
    assert.strictEqual(activity.durationEstimated, false);
    assert.strictEqual(activity.ageRange, "0-8 years");
    assert.strictEqual(activity.isFree, false);
    assert.strictEqual(activity.cost, "Included with park admission");
    assert.strictEqual(activity.isIndoor, false);
    assert.strictEqual(activity.venueType, "park");
    assert.strictEqual(activity.source, "tec");
    assert.strictEqual(activity.sourceUrl, "https://example-park.org/event/puppet-show/2026-10-14/");
});

test("narrower audiences, free events and show lengths", () => {
    const story = build(103).activity;
    assert.strictEqual(story.ageRange, "0-3 years");
    assert.strictEqual(story.isFree, true);
    assert.strictEqual(story.cost, "Free");
    const show = build(106).activity;
    assert.strictEqual(show.ageRange, "All", "\"All ages\" note");
    assert.strictEqual(show.durationMinutes, 16, "16-minute show in a 1-hour slot");
    assert.strictEqual(show.description, "16 minutes All ages Explores the nature of Earth’s moon.");
    assert.strictEqual(tec.showLengthMinutes("(25 min) Ever wondered"), 25);
    assert.strictEqual(tec.showLengthMinutes("Playing at 11 a.m."), null);
});

test("cancelled events are reported so a saved copy can be removed", () => {
    assert.deepStrictEqual(build(107), { activityId: "tec_examplepark_107", activity: null, cancelled: true });
});

test("fetchAllEvents pages through the API with the crawl delay", async () => {
    const urls = [];
    const waits = [];
    const fetchImpl = async (url) => {
        urls.push(url);
        const n = Number(new URL(url).searchParams.get("page"));
        return { ok: true, status: 200, json: async () => page(n) };
    };
    const { events, complete } = await tec.fetchAllEvents(SITE, {
        fromSec: NOW, toSec: NOW + 60 * 86400, fetchImpl, sleep: async (ms) => waits.push(ms),
    });
    assert.strictEqual(events.length, 7);
    assert.strictEqual(complete, true);
    assert.strictEqual(urls.length, 2);
    assert.match(urls[0], /\/wp-json\/tribe\/events\/v1\/events\?start_date=2026-10-13&end_date=2026-12-12&per_page=50&page=1$/);
    assert.deepStrictEqual(waits, [10000], "10 s between pages");
});

test("fetchAllEvents reports an incomplete read at the page cap", async () => {
    const fetchImpl = async () => ({ ok: true, status: 200, json: async () => page(1) });
    const { complete } = await tec.fetchAllEvents(SITE, { fromSec: NOW, toSec: NOW, fetchImpl, sleep: async () => {}, maxPages: 1 });
    assert.strictEqual(complete, false);
});

test("Gemini-scraped activities from the site are superseded", () => {
    assert.strictEqual(tec.isSupersededUrl("https://www.example-park.org/events", SITE), true);
    assert.strictEqual(tec.isSupersededUrl("https://other.org/events", SITE), false);
    assert.strictEqual(tec.isSupersededUrl("not a url", SITE), false);
});

test("disabled sites keep their config but are not synced", () => {
    const ids = tec.enabledSites().map((s) => s.id);
    assert.ok(ids.includes("fairyland"));
    assert.ok(!ids.includes("chabot"), "Chabot blocks Google Cloud requests");
    assert.ok(tec.TEC_SITES.some((s) => s.id === "chabot"));
});
