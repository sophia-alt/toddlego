const admin = require("firebase-admin");
const crypto = require("crypto");
const { db, toDateKey } = require("./helpers");

/**
 * Recurring series: most toddler programs are the same storytime every week at the same
 * branch and time. Each one gets a doc in `series` with an RRULE, and its activities point to
 * it via `seriesId`, so the app can show a weekly timetable.
 *
 * The rule is inferred from the dated sessions a source lists, never guessed by the LLM:
 * sessions with the same venue, title, weekday and start time whose dates are 7 (or 14) days
 * apart form a series. A single session joins a series found on an earlier run.
 */

const WEEKDAYS = ["SU", "MO", "TU", "WE", "TH", "FR", "SA"];

/** All sessions are local to California; RRULE times are interpreted in this zone. */
const SERIES_TIMEZONE = "America/Los_Angeles";

/** A series whose sessions stop appearing is deleted (TTL on expireAt) this long after its last one. */
const SERIES_TTL_SEC = 30 * 24 * 60 * 60;

const normalizeTitle = (s) => String(s).trim().toLowerCase().replace(/\s+/g, " ");

/** Weekday and local start time of a session, e.g. { dateKey: "2026-10-14", byDay: "WE", localTime: "10:30" }. */
function weeklySlot(startTimeSec) {
    const d = new Date(startTimeSec * 1000);
    return {
        dateKey: toDateKey(d),
        byDay: WEEKDAYS[d.getDay()],
        localTime: `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`,
    };
}

const seriesIdFor = (venueId, title, byDay, localTime) =>
    crypto.createHash("sha256")
        .update([venueId, normalizeTitle(title), byDay, localTime].join("|"), "utf8")
        .digest("hex")
        .substring(0, 40);

/** Days since epoch for "YYYY-MM-DD", independent of DST. */
const dayNumber = (dateKey) => {
    const [y, m, d] = dateKey.split("-").map(Number);
    return Date.UTC(y, m - 1, d) / 86400000;
};

const gcd = (a, b) => (b === 0 ? a : gcd(b, a % b));

/**
 * Week interval (1 or 2) of sorted, distinct session dates, or null if they are not a weekly
 * pattern. Missing weeks (e.g. a holiday) still count as weekly: 7 and 14 day gaps give 1.
 */
function weeklyInterval(dateKeys) {
    if (dateKeys.length < 2) return null;
    let g = 0;
    for (let i = 1; i < dateKeys.length; i++) {
        g = gcd(dayNumber(dateKeys[i]) - dayNumber(dateKeys[i - 1]), g);
    }
    if (g === 7) return 1;
    if (g === 14) return 2;
    return null;
}

const buildRrule = (byDay, interval) =>
    interval === 1 ? `FREQ=WEEKLY;BYDAY=${byDay}` : `FREQ=WEEKLY;INTERVAL=${interval};BYDAY=${byDay}`;

/**
 * Groups sessions by venue, title, weekday and start time.
 * @param {Array<{venueId: string, title: string, startTime: number}>} occurrences
 * @returns {Array<{seriesId, byDay, localTime, dateKeys: string[], members: number[], interval, rrule}>}
 *   `members` are indexes into `occurrences`; `rrule` is null when the group is not weekly.
 */
function groupIntoSeries(occurrences) {
    const groups = new Map();
    occurrences.forEach((o, i) => {
        if (!o || !o.venueId || !o.title || !o.startTime) return;
        const { dateKey, byDay, localTime } = weeklySlot(o.startTime);
        const seriesId = seriesIdFor(o.venueId, o.title, byDay, localTime);
        if (!groups.has(seriesId)) {
            groups.set(seriesId, { seriesId, byDay, localTime, dateKeys: new Set(), members: [] });
        }
        const g = groups.get(seriesId);
        g.dateKeys.add(dateKey);
        g.members.push(i);
    });
    return [...groups.values()].map((g) => {
        const dateKeys = [...g.dateKeys].sort();
        const interval = weeklyInterval(dateKeys);
        return { ...g, dateKeys, interval, rrule: interval ? buildRrule(g.byDay, interval) : null };
    });
}

/**
 * Finds the series of each session, queues the series writes on `batch`, and returns
 * one { seriesId, rrule } (or null) per occurrence, in order.
 * @param {FirebaseFirestore.WriteBatch} batch
 * @param {Array<{venueId, venueName, title, startTime, endTime, sourceUrl, ageRange,
 *   latitude?, longitude?, geohash?}>} occurrences
 *   Every session the source listed, including ones already saved, so the pattern is complete.
 *   Use seriesOccurrence() to build these from a resolved venue.
 */
async function linkSeries(batch, occurrences) {
    const links = occurrences.map(() => null);
    const groups = groupIntoSeries(occurrences);

    // A lone session can still belong to a series seen on an earlier run
    const singles = groups.filter((g) => !g.rrule);
    const stored = new Map();
    if (singles.length > 0) {
        const snaps = await db.getAll(...singles.map((g) => db.collection("series").doc(g.seriesId)));
        snaps.forEach((s) => s.exists && stored.set(s.id, s.data()));
    }

    const now = Math.floor(Date.now() / 1000);
    for (const g of groups) {
        const prior = stored.get(g.seriesId);
        const rrule = g.rrule || (prior && prior.rrule);
        if (!rrule) continue;

        const sessions = g.members.map((i) => occurrences[i]);
        let lastStart = Math.max(...sessions.map((s) => s.startTime));
        if (prior && prior.lastSessionAt > lastStart) lastStart = prior.lastSessionAt;

        const update = {
            occurrenceDates: admin.firestore.FieldValue.arrayUnion(...g.dateKeys),
            lastSeenAt: now,
            lastSessionAt: lastStart,
            expireAt: new Date((lastStart + SERIES_TTL_SEC) * 1000),
        };
        if (g.rrule) {
            // This run saw the pattern itself: (re)write the series description
            const first = sessions[0];
            Object.assign(update, {
                title: first.title,
                venueId: first.venueId,
                venue: first.venueName,
                rrule: g.rrule,
                byDay: g.byDay,
                interval: g.interval,
                localStartTime: g.localTime,
                timezone: SERIES_TIMEZONE,
                durationMinutes: Math.max(0, Math.round((first.endTime - first.startTime) / 60)),
                durationEstimated: !!first.durationEstimated,
                ageRange: first.ageRange ?? null,
                sourceUrl: first.sourceUrl ?? null,
            });
            // The venue's coordinates, so the app's weekly timetable can filter series by distance
            if (Number.isFinite(first.latitude) && Number.isFinite(first.longitude)) {
                Object.assign(update, {
                    latitude: first.latitude,
                    longitude: first.longitude,
                    geohash: first.geohash ?? null,
                });
            }
        }
        batch.set(db.collection("series").doc(g.seriesId), update, { merge: true });
        for (const i of g.members) links[i] = { seriesId: g.seriesId, rrule };
    }
    return links;
}

/**
 * One session for linkSeries(), with the venue's ID and coordinates.
 * @param {{venueId: string, lat: number, lng: number, geohash: string}} venue - From resolveVenue().
 */
const seriesOccurrence = (venue, { venueName, title, startTime, endTime, durationEstimated, sourceUrl, ageRange }) => ({
    venueId: venue.venueId,
    venueName,
    title,
    startTime,
    endTime,
    durationEstimated: !!durationEstimated,
    sourceUrl: sourceUrl ?? null,
    ageRange,
    latitude: venue.lat,
    longitude: venue.lng,
    geohash: venue.geohash,
});

module.exports = {
    SERIES_TIMEZONE,
    seriesOccurrence,
    weeklySlot,
    seriesIdFor,
    weeklyInterval,
    buildRrule,
    groupIntoSeries,
    linkSeries,
};
