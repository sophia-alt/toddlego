// Offline tests for the BiblioCommons RSS adapter (sources/bibliocommons.js).
// Uses a synthetic feed fixture and a fake fetch; no network, Firestore or paid APIs.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const bc = require("../sources/bibliocommons");

const FEED = fs.readFileSync(path.join(__dirname, "fixtures", "bibliocommons-feed.xml"), "utf8");
const EMPTY_FEED = FEED.replace(/<item>[\s\S]*<\/item>\n/g, "");
const NOW = Math.floor(Date.parse("2026-10-13T00:00:00Z") / 1000);

test("parseFeed reads library name, items and bc: fields", async () => {
    const { libraryName, items } = await bc.parseFeed(FEED);
    assert.strictEqual(libraryName, "Example County Library");
    assert.strictEqual(items.length, 6);
    const baby = items[0];
    assert.strictEqual(baby.id, "aaa111");
    assert.strictEqual(baby.title, "Baby Bounce");
    assert.deepStrictEqual(baby.categories, ["Storytime", "Babies & Toddlers", "English"]);
    assert.strictEqual(baby.startUtc, "2026-10-14T17:30:00Z");
    assert.strictEqual(baby.location.latitude, 37.55);
    assert.strictEqual(baby.location.details, "Storytime Room");
    assert.strictEqual(items[3].isCancelled, true);
    assert.strictEqual(items[4].isVirtual, true);
    assert.strictEqual(items[5].registration.full, true);
});

test("classifyAudience handles each library's labels", () => {
    const age = (cats) => bc.classifyAudience(cats).ageRange;
    assert.strictEqual(age(["Babies & Toddlers"]), "0-3 years");
    assert.strictEqual(age(["Kids: Babies", "Kids: Preschoolers"]), "0-5 years");
    assert.strictEqual(age(["Young Children, ages 0-5"]), "0-5 years");
    assert.strictEqual(age(["Birth to 5"]), "0-5 years");
    assert.strictEqual(age(["Preschoolers (0-5)"]), "0-5 years");
    assert.strictEqual(age(["Pre-schoolers (3-5)"]), "3-5 years");
    assert.strictEqual(age(["Families"]), "All");
    assert.strictEqual(bc.classifyAudience(["Adults"]).include, false);
    assert.strictEqual(bc.classifyAudience(["Kids, ages 5-10"]).include, false);
    assert.strictEqual(bc.classifyAudience(["Kids: Toddlers", "Scheduled Closure/Holiday"]).include, false);
});

test("classifyAudience drops meal services and school-age academic programs", () => {
    // Real label combinations from Bay Area feeds
    assert.strictEqual(bc.classifyAudience(["Free Student Meals", "Snacks and Meals", "Babies (Birth to 1 year)"], "After School Meals").include, false);
    assert.strictEqual(bc.classifyAudience(["Meals at the Library", "Kids: Babies"], "Free Snacks for Kids!").include, false);
    assert.strictEqual(bc.classifyAudience(["Community Resources", "Preschoolers (0-5)"], "Free Meals at the Library").include, false);
    assert.strictEqual(bc.classifyAudience(["Academic Support & Homework Help", "Young Children, ages 0-5"], "OneinMath: Math Program for K-3rd").include, false);
    assert.strictEqual(bc.classifyAudience(["Kids, ages 5-10", "Young Children, ages 0-5"], "Homework Help").include, false);
    assert.strictEqual(bc.classifyAudience(["Families", "Tweens"], "Tween Club").include, false);
    // SJPL files storytimes and reading programs under "Academic Support & Homework Help" too
    assert.strictEqual(bc.classifyAudience(["Academic Support & Homework Help", "Storytime", "Young Children, ages 0-5"], "Vietnamese Storytime & Craft").include, true);
    assert.strictEqual(bc.classifyAudience(["Academic Support & Homework Help", "Young Children, ages 0-5"], "Reading to Furry Friends").include, true);
    // Young-children programs that mention older ages stay in
    assert.strictEqual(bc.classifyAudience(["Kids: Preschoolers", "Kids: Grades K-8"], "Story Explorers: TK-1st Grade Book Club").include, true);
    assert.strictEqual(bc.classifyAudience(["Kids: Babies", "Kids: Toddlers"], "Lil' Athletes for Ages 18 Months - 4 Years").include, true);
});

test("buildActivity flags date-only (all-day) library entries as all-day", async () => {
    const { libraryName, items } = await bc.parseFeed(FEED);
    // As in real feeds: local start has no time; UTC spans the local day(s)
    const allDay = { ...items[0], startLocal: "2026-10-17", startUtc: "2026-10-17T07:00:00Z", endUtc: "2026-10-18T06:59:59Z" };
    assert.strictEqual(bc.buildActivity("examplelib", libraryName, allDay, { nowSec: NOW }).activity.isAllDay, true);
    assert.strictEqual(bc.buildActivity("examplelib", libraryName, items[0], { nowSec: NOW }).activity.isAllDay, false);
});

test("classifyAudience drops school-age programs filed only under families", () => {
    assert.strictEqual(bc.classifyAudience(["Kids: Family Events"], "Homework Help").include, false);
    assert.strictEqual(bc.classifyAudience(["Families"], "After-School Snack").include, false);
    // An explicit young-children audience is trusted over the title
    assert.strictEqual(bc.classifyAudience(["Young Children, ages 0-5"], "After-School Storytime").include, true);
    assert.strictEqual(bc.classifyAudience(["Families"], "Family Storytime").include, true);
});

test("isYoungChildAudience picks the audience IDs to request", () => {
    for (const yes of ["Babies (under 2)", "Toddlers (18 mos. to 3 yrs)", "Pre-schoolers (3-5)", "Birth to 5", "Kids: Family Events"]) {
        assert.ok(bc.isYoungChildAudience(yes), yes);
    }
    for (const no of ["Adults", "Teens (12-18)", "Kids (6-11)", "Seniors", "All Ages"]) {
        assert.ok(!bc.isYoungChildAudience(no), no);
    }
});

test("buildActivity maps a toddler event without LLM or geocoding", async () => {
    const { libraryName, items } = await bc.parseFeed(FEED);
    const b = bc.buildActivity("examplelib", libraryName, items[0], { nowSec: NOW });
    assert.strictEqual(b.activityId, "bc_examplelib_aaa111");
    assert.strictEqual(b.activity.venue, "Example County Library - Main Branch");
    assert.strictEqual(b.activity.startTime, Date.parse("2026-10-14T17:30:00Z") / 1000);
    assert.strictEqual(b.activity.endTime - b.activity.startTime, 30 * 60);
    assert.strictEqual(b.activity.ageRange, "0-3 years");
    assert.deepStrictEqual(b.activity.age_range, [0, 3]);
    assert.strictEqual(b.activity.sourceUrl, "https://examplelib.bibliocommons.com/events/aaa111");
    assert.strictEqual(b.activity.description, "Songs & rhymes for babies Join us for bouncing rhymes, songs and a short story. Bring a blanket!");
    assert.strictEqual(b.activity.lastVerifiedAt, NOW);
    assert.deepStrictEqual(b.venue, { name: "Example County Library - Main Branch", lat: 37.55, lng: -121.97, address: "100 Library Way, Exampleville, CA 94000" });
});

test("buildActivity skips adults, closures, cancelled and coordinate-less events", async () => {
    const { libraryName, items } = await bc.parseFeed(FEED);
    const ids = items.filter((it) => bc.buildActivity("examplelib", libraryName, it, { nowSec: NOW })).map((it) => it.id);
    assert.deepStrictEqual(ids, ["aaa111", "fff666"]);
});

test("buildActivity defaults a missing end time to one hour and links registration", async () => {
    const { libraryName, items } = await bc.parseFeed(FEED);
    const b = bc.buildActivity("examplelib", libraryName, items[5], { nowSec: NOW });
    assert.strictEqual(b.activity.endTime - b.activity.startTime, 60 * 60);
    assert.strictEqual(b.activity.ageRange, "3-5 years");
    assert.strictEqual(b.activity.requiresBooking, true);
    assert.strictEqual(b.activity.isFull, true);
    assert.strictEqual(b.activity.registrationUrl, b.activity.sourceUrl);
});

test("excerpt strips HTML and shortens at a word boundary", () => {
    assert.strictEqual(bc.excerpt("<p>a &amp; b</p>"), "a & b");
    const long = bc.excerpt(`<p>${"word ".repeat(100)}</p>`, 40);
    assert.ok(long.length <= 41 && long.endsWith("…"), long);
});

test("isSupersededUrl matches the library's BiblioCommons site and website only", () => {
    const config = { tenant: "aclibrary", websiteHosts: ["aclibrary.org"] };
    assert.ok(bc.isSupersededUrl("https://aclibrary.bibliocommons.com/v2/events?locations=FRM", config));
    assert.ok(bc.isSupersededUrl("https://www.aclibrary.org/locations/fremont", config));
    assert.ok(!bc.isSupersededUrl("https://sjpl.bibliocommons.com/v2/events", config));
    assert.ok(!bc.isSupersededUrl("https://hayward.librarycalendar.com/events", config));
    assert.ok(!bc.isSupersededUrl("not a url", config));
});

test("fetchAllItems pages until an empty page and passes the audience filter", async () => {
    const urls = [];
    const pages = [FEED, FEED.replace(/aaa111|bbb222|ccc333|ddd444|eee555|fff666/g, (id) => id + "x"), EMPTY_FEED];
    const fetchImpl = async (url) => {
        urls.push(url);
        return { ok: true, text: async () => pages[urls.length - 1] };
    };
    const { libraryName, items, complete } = await bc.fetchAllItems("examplelib", { audienceIds: ["A", "B"], delayMs: 0, fetchImpl });
    assert.strictEqual(libraryName, "Example County Library");
    assert.strictEqual(items.length, 12);
    assert.strictEqual(complete, true);
    assert.strictEqual(urls.length, 3);
    assert.match(urls[0], /\/examplelib\/rss\/events\?page=1&audiences=A,B$/);
});

test("fetchAllItems reports an incomplete read when the page cap is hit", async () => {
    let n = 0;
    const fetchImpl = async () => ({ ok: true, text: async () => FEED.replace(/<link>https:\/\/examplelib\.bibliocommons\.com\/events\/(\w+)/g, (m, id) => m + "p" + n++) });
    const { complete } = await bc.fetchAllItems("examplelib", { delayMs: 0, maxPages: 2, fetchImpl });
    assert.strictEqual(complete, false);
});

test("fetchAllItems throws on HTTP errors instead of returning a partial feed", async () => {
    const fetchImpl = async () => ({ ok: false, status: 503 });
    await assert.rejects(() => bc.fetchAllItems("examplelib", { delayMs: 0, fetchImpl }), /HTTP 503/);
});
