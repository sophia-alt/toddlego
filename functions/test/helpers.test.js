// Offline unit tests for utils/helpers.js. Run with: npm test
// Nothing here touches Firestore or any paid API.
process.env.TZ = "America/Los_Angeles"; // same as index.js, before any Date is created
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const {
    generateEventId,
    generateLegacyEventId,
    isSameOccurrence,
    toDateKey,
    toDateTimeKey,
    parseDate,
    getReusableParsedEvents,
} = require("../utils/helpers");

const TITLE = "Baby Bounce";
const VENUE = "Alameda County Library - Dublin";

test("legacy IDs collide across dates (the bug being fixed)", () => {
    assert.strictEqual(
        generateLegacyEventId(TITLE, VENUE, "2026-10-14"),
        generateLegacyEventId(TITLE, VENUE, "2026-10-21"),
    );
});

test("event IDs differ for each weekly session", () => {
    assert.notStrictEqual(
        generateEventId(TITLE, VENUE, "2026-10-14T10:00"),
        generateEventId(TITLE, VENUE, "2026-10-21T10:00"),
    );
});

test("event IDs differ for two sessions on the same day", () => {
    assert.notStrictEqual(
        generateEventId(TITLE, VENUE, "2026-10-14T10:00"),
        generateEventId(TITLE, VENUE, "2026-10-14T11:00"),
    );
});

test("event IDs differ for long venue names sharing a prefix", () => {
    const title = "Toddler Storytime";
    assert.notStrictEqual(
        generateEventId(title, "Alameda County Library - Fremont Main", "2026-10-14"),
        generateEventId(title, "Alameda County Library - Newark", "2026-10-14"),
    );
});

test("event IDs are stable and ignore case/whitespace differences", () => {
    const id = generateEventId(TITLE, VENUE, "2026-10-14T10:00");
    assert.strictEqual(id, generateEventId("  baby   bounce ", "alameda county library - dublin", "2026-10-14T10:00"));
    assert.match(id, /^[0-9a-f]{40}$/);
});

test("generateEventId rejects missing parts", () => {
    assert.throws(() => generateEventId(TITLE, "", "2026-10-14"));
});

test("date keys use Pacific time", () => {
    // 2026-10-14T06:30Z is 23:30 on Oct 13 in California
    const d = new Date("2026-10-14T06:30:00Z");
    assert.strictEqual(toDateKey(d), "2026-10-13");
    assert.strictEqual(toDateTimeKey(d), "2026-10-13T23:30");
});

test("isSameOccurrence matches only the same day, title and venue", () => {
    const startTime = Math.floor(new Date("2026-10-14T10:00:00").getTime() / 1000);
    const stored = { title: "Baby Bounce", venue: "Alameda County Library - Dublin", startTime };
    assert.ok(isSameOccurrence(stored, " baby bounce", "ALAMEDA COUNTY LIBRARY - DUBLIN", "2026-10-14"));
    assert.ok(!isSameOccurrence(stored, TITLE, VENUE, "2026-10-21"), "different week");
    assert.ok(!isSameOccurrence(stored, TITLE, "Alameda County Library - Dublin West", "2026-10-14"), "different venue");
    assert.ok(!isSameOccurrence({ title: TITLE, venue: VENUE }, TITLE, VENUE, "2026-10-14"), "no startTime");
});

test("parseDate keeps date-only strings on the same calendar day", () => {
    const d = parseDate("2026-10-12");
    assert.deepStrictEqual([d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours()], [2026, 10, 12, 12]);
    assert.strictEqual(parseDate("TBD"), null);
});

test("parsed-page cache is reused only when unchanged and recent", () => {
    const now = Math.floor(Date.now() / 1000);
    const parsed = JSON.stringify([{ title: "Storytime" }]);
    assert.deepStrictEqual(
        getReusableParsedEvents({ content_hash: "a", last_parsed: now - 3600, parsed_json: parsed }, "a"),
        [{ title: "Storytime" }],
    );
    assert.strictEqual(getReusableParsedEvents({ content_hash: "a", last_parsed: now - 8 * 86400, parsed_json: parsed }, "a"), null);
    assert.strictEqual(getReusableParsedEvents({ content_hash: "b", last_parsed: now, parsed_json: parsed }, "a"), null);
    assert.strictEqual(getReusableParsedEvents({ content_hash: "a", last_parsed: now }, "a"), null);
});
