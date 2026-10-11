// Offline unit tests for utils/venues.js and utils/series.js. Run with: npm test
// Nothing here touches Firestore or any paid API.
process.env.TZ = "America/Los_Angeles"; // same as index.js, before any Date is created
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const { normalizeVenueName, venueIdFor, knownCoordsFor } = require("../utils/venues");
const { weeklySlot, weeklyInterval, groupIntoSeries, linkSeries, seriesOccurrence } = require("../utils/series");

/** Unix seconds for a local (Pacific) date-time string. */
const at = (local) => Math.floor(new Date(local).getTime() / 1000);

const DUBLIN = "name_dublin";
const babyBounce = (local, venueId = DUBLIN, title = "Baby Bounce") => ({
    venueId, venueName: "Alameda County Library - Dublin", title,
    startTime: at(local), endTime: at(local) + 30 * 60, sourceUrl: "https://example.org", ageRange: "0-18 months",
});

test("venue spellings that differ only in case and punctuation share a key", () => {
    const key = normalizeVenueName("Alameda County Library - Dublin");
    assert.strictEqual(normalizeVenueName("alameda county library, Dublin"), key);
    assert.strictEqual(normalizeVenueName("  Alameda County Library – Dublin. "), key);
    assert.strictEqual(normalizeVenueName("Parks & Rec"), normalizeVenueName("Parks and Rec"));
    assert.notStrictEqual(normalizeVenueName("Alameda County Library - Dublin West"), key);
});

test("venue IDs prefer the Google place over the name", () => {
    assert.strictEqual(venueIdFor("ChIJabc", "a"), venueIdFor("ChIJabc", "b"));
    assert.match(venueIdFor(null, "a"), /^name_[0-9a-f]{32}$/);
    assert.match(venueIdFor("ChIJabc", "a"), /^place_[0-9a-f]{32}$/);
});

test("a source's known coordinates apply only to events at its own venue", () => {
    const coords = { lat: 37.33, lng: -121.88 };
    // Same venue, spelled differently
    assert.strictEqual(knownCoordsFor("Willow Glen Branch Library", "willow glen branch library", coords), coords);
    // Another branch listed on the same library page must not inherit these coordinates
    assert.strictEqual(knownCoordsFor("San Jose Public Library - Almaden", "Willow Glen Branch Library", coords), null);
    assert.strictEqual(knownCoordsFor("Willow Glen Branch Library", undefined, coords), null);
    assert.strictEqual(knownCoordsFor("Willow Glen Branch Library", "Willow Glen Branch Library", null), null);
});

test("weekly slot uses Pacific weekday and time", () => {
    assert.deepStrictEqual(weeklySlot(at("2026-10-14T10:30:00")), { dateKey: "2026-10-14", byDay: "WE", localTime: "10:30" });
});

test("weekly interval: weekly, biweekly, skipped weeks, and non-weekly dates", () => {
    assert.strictEqual(weeklyInterval(["2026-10-14", "2026-10-21", "2026-10-28"]), 1);
    assert.strictEqual(weeklyInterval(["2026-10-14", "2026-10-28"]), 2);
    assert.strictEqual(weeklyInterval(["2026-10-14", "2026-10-21", "2026-11-04"]), 1, "holiday week skipped");
    assert.strictEqual(weeklyInterval(["2026-10-28", "2026-11-04"]), 1, "across the DST change");
    assert.strictEqual(weeklyInterval(["2026-10-14", "2026-10-15"]), null);
    assert.strictEqual(weeklyInterval(["2026-10-14"]), null);
});

test("sessions group by venue, title, weekday and start time", () => {
    const groups = groupIntoSeries([
        babyBounce("2026-10-14T10:30:00"),
        babyBounce("2026-10-21T10:30:00"),
        babyBounce("2026-10-14T11:30:00"), // second session the same morning
        babyBounce("2026-10-14T10:30:00", "name_fremont"),
        babyBounce("2026-10-14T10:30:00", DUBLIN, "Toddler Storytime"),
        { title: "No venue", startTime: at("2026-10-14T10:30:00") },
    ]);
    assert.strictEqual(groups.length, 4);
    const weekly = groups.find((g) => g.members.length === 2);
    assert.deepStrictEqual(weekly.members, [0, 1]);
    assert.strictEqual(weekly.rrule, "FREQ=WEEKLY;BYDAY=WE");
    assert.strictEqual(groups.filter((g) => g.rrule).length, 1);
});

test("linkSeries links weekly sessions and writes one series doc", async () => {
    const writes = [];
    const batch = { set: (ref, data, opts) => writes.push({ id: ref.id, path: ref.parent.id, data, opts }) };
    const links = await linkSeries(batch, [
        babyBounce("2026-10-14T10:30:00"),
        babyBounce("2026-10-21T10:30:00"),
        babyBounce("2026-10-28T10:30:00"),
    ]);
    assert.strictEqual(writes.length, 1);
    assert.strictEqual(writes[0].path, "series");
    assert.deepStrictEqual(writes[0].opts, { merge: true });
    assert.strictEqual(writes[0].data.rrule, "FREQ=WEEKLY;BYDAY=WE");
    assert.strictEqual(writes[0].data.localStartTime, "10:30");
    assert.strictEqual(writes[0].data.durationMinutes, 30);
    assert.strictEqual(writes[0].data.venueId, DUBLIN);
    assert.ok(links.every((l) => l && l.seriesId === writes[0].id && l.rrule === "FREQ=WEEKLY;BYDAY=WE"));
});

test("series docs carry the venue's coordinates for distance filtering", async () => {
    const writes = [];
    const batch = { set: (ref, data) => writes.push(data) };
    const venue = { venueId: DUBLIN, lat: 37.7, lng: -121.9, geohash: "9q9pxyz" };
    const session = (local) => seriesOccurrence(venue, {
        venueName: "Alameda County Library - Dublin", title: "Baby Bounce",
        startTime: at(local), endTime: at(local) + 30 * 60, sourceUrl: "https://example.org", ageRange: "0-18 months",
    });
    await linkSeries(batch, [session("2026-10-14T10:30:00"), session("2026-10-21T10:30:00")]);
    assert.strictEqual(writes.length, 1);
    assert.deepStrictEqual([writes[0].latitude, writes[0].longitude, writes[0].geohash], [37.7, -121.9, "9q9pxyz"]);
    assert.strictEqual(writes[0].venueId, DUBLIN);
});
