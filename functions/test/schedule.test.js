// Offline tests for utils/schedule.js: expanding Gemini's per-program schedules to sessions.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const { expandToSessions, normalizeTime, scheduleInstructions, PARSE_VERSION } = require("../utils/schedule");
const { getReusableParsedEvents } = require("../utils/helpers");

const TODAY = "2026-10-12"; // a Monday
const at = (program) => expandToSessions(program, { today: TODAY, horizonDays: 28 });
const dates = (sessions) => sessions.map((s) => s.isoDate);

const storytime = { title: "Toddler Storytime", venue: "Example Library", startTime: "10:30", endTime: "11:00" };

test("a weekly rule becomes the next four weeks of sessions", () => {
    const s = at({ ...storytime, recurrence: { byDay: ["WE"], intervalWeeks: 1 } });
    assert.deepStrictEqual(dates(s), [
        "2026-10-14T10:30:00", "2026-10-21T10:30:00", "2026-10-28T10:30:00", "2026-11-04T10:30:00",
    ]);
    assert.strictEqual(s[0].endTime, "2026-10-14T11:00:00");
    assert.ok(s.every((x) => x.scheduleSource === "recurrence"));
    assert.strictEqual(s[0].title, "Toddler Storytime");
    assert.ok(!("recurrence" in s[0]) && !("dates" in s[0]), "schedule fields are not copied to sessions");
});

test("several weekdays, lowercase or full day names", () => {
    const s = at({ ...storytime, recurrence: { byDay: ["tu", "Thursday"] } });
    assert.deepStrictEqual(dates(s).slice(0, 3), ["2026-10-13T10:30:00", "2026-10-15T10:30:00", "2026-10-20T10:30:00"]);
    assert.strictEqual(s.length, 8);
});

test("start and end dates bound the rule", () => {
    const s = at({ ...storytime, recurrence: { byDay: ["WE"], startDate: "2026-10-20", endDate: "2026-10-31" } });
    assert.deepStrictEqual(dates(s), ["2026-10-21T10:30:00", "2026-10-28T10:30:00"]);
});

test("every other week follows the first session; without one it is not guessed", () => {
    const anchored = at({ ...storytime, recurrence: { byDay: ["WE"], intervalWeeks: 2, startDate: "2026-10-07" } });
    assert.deepStrictEqual(dates(anchored), ["2026-10-21T10:30:00", "2026-11-04T10:30:00"]);
    const unanchored = at({ ...storytime, recurrence: { byDay: ["WE"], intervalWeeks: 2 }, dates: ["2026-10-14"] });
    assert.deepStrictEqual(dates(unanchored), ["2026-10-14T10:30:00"]);
    assert.deepStrictEqual(at({ ...storytime, recurrence: { byDay: ["WE"], intervalWeeks: 4 } }), []);
});

test("listed dates and rule dates merge, labeled by source; except dates are removed", () => {
    const s = at({
        ...storytime,
        dates: ["2026-10-14", "2026-10-17"],
        recurrence: { byDay: ["WE"] },
        exceptDates: ["2026-10-28"],
    });
    assert.deepStrictEqual(s.map((x) => [x.isoDate.slice(0, 10), x.scheduleSource]), [
        ["2026-10-14", "listed"],
        ["2026-10-17", "listed"],
        ["2026-10-21", "recurrence"],
        ["2026-11-04", "recurrence"],
    ]);
});

test("one-time and monthly programs use their listed dates only", () => {
    const s = at({ ...storytime, dates: ["2026-11-03", "2026-12-01"], recurrence: null });
    assert.deepStrictEqual(dates(s), ["2026-11-03T10:30:00", "2026-12-01T10:30:00"]);
    assert.ok(s.every((x) => x.scheduleSource === "listed"));
});

test("programs without a valid start time, dates or rule produce nothing", () => {
    assert.deepStrictEqual(at({ ...storytime, startTime: null, recurrence: { byDay: ["WE"] } }), []);
    assert.deepStrictEqual(at({ ...storytime, startTime: "25:00", dates: ["2026-10-14"] }), []);
    assert.deepStrictEqual(at({ ...storytime, dates: ["2026-02-30", "soon"] }), []);
    assert.deepStrictEqual(at({ ...storytime, recurrence: { byDay: ["XX"] } }), []);
    assert.deepStrictEqual(at(null), []);
});

test("an end time not after the start is dropped", () => {
    const [s] = at({ ...storytime, endTime: "10:00", dates: ["2026-10-14"] });
    assert.strictEqual(s.endTime, null);
});

test("per-session entries cached before PARSE_VERSION 2 pass through", () => {
    const old = { title: "Baby Bounce", venue: "Example Library", isoDate: "2026-10-14T10:00:00" };
    assert.deepStrictEqual(at(old), [{ ...old, scheduleSource: "listed" }]);
});

test("normalizeTime pads hours and rejects invalid times", () => {
    assert.strictEqual(normalizeTime("9:05"), "09:05");
    assert.strictEqual(normalizeTime("18:30"), "18:30");
    assert.strictEqual(normalizeTime("6pm"), null);
    assert.strictEqual(normalizeTime(undefined), null);
});

test("scheduleInstructions tells the model today's date and weekday", () => {
    const text = scheduleInstructions(new Date(2026, 9, 12, 9, 0));
    assert.match(text, /Today is 2026-10-12 \(Monday\)/);
    assert.match(text, /ONE entry per program/);
});

test("parses cached in an older format are not reused", () => {
    const now = Math.floor(Date.now() / 1000);
    const cache = { content_hash: "a", last_parsed: now, parsed_json: "[]" };
    assert.strictEqual(getReusableParsedEvents(cache, "a", PARSE_VERSION), null);
    assert.deepStrictEqual(getReusableParsedEvents({ ...cache, parse_version: PARSE_VERSION }, "a", PARSE_VERSION), []);
});
