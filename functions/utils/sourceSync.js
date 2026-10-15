// Writes one tenant's mapped activities from a structured source (LibCal, Communico) to
// Firestore: venues from known coordinates, weekly series, upserts, and removal of
// cancelled, unlisted and superseded activities.

const { db } = require("./helpers");
const { resolveVenue, venueLocationFields } = require("./venues");
const { linkSeries, seriesOccurrence } = require("./series");
const { writeQueue } = require("./writeQueue");

/** Only events in the next 60 days are stored; later runs pick up later ones. */
const HORIZON_SEC = 60 * 24 * 60 * 60;

/** Activities other jobs scraped with Gemini; replaced by structured sources they overlap. */
const SUPERSEDED_SOURCES = ["city_calendar", "web_scrape"];

/**
 * Sorts a source's mapped events into activities to store, cancelled IDs, and skipped.
 * @param {Array<null|{activityId, activity, venue, cancelled?}>} results - buildActivity output.
 */
function partition(results, nowSec) {
    const built = [];
    const cancelledIds = [];
    let skipped = 0;
    for (const b of results) {
        if (b && b.cancelled) {
            cancelledIds.push(b.activityId);
        } else if (!b || b.activity.endTime < nowSec || b.activity.startTime > nowSec + HORIZON_SEC) {
            skipped++;
        } else {
            built.push(b);
        }
    }
    return { built, cancelledIds, skipped };
}

/**
 * Stores one tenant's activities.
 * @param {Object} p
 * @param {string} p.source - Activity `source` field ("libcal", "communico").
 * @param {string} p.tenant
 * @param {Array} p.built - From partition(); each has activityId, activity and venue
 *   ({name, lat, lng, address}).
 * @param {string[]} p.cancelledIds
 * @param {boolean} p.complete - Whether the source was read in full; unlisted upcoming
 *   activities are only removed after a complete read.
 * @param {number} p.nowSec
 * @param {(url: string) => boolean} p.isSuperseded - Whether a Gemini-scraped activity's
 *   source URL is covered by this tenant.
 */
async function storeActivities({ source, tenant, built, cancelledIds, complete, nowSec, isSuperseded }) {
    // Venues: coordinates come from the source config, so this never geocodes
    const memo = new Map();
    const occurrences = [];
    let skipped = 0;
    for (const b of built) {
        const venue = await resolveVenue(b.venue.name, { memo, knownCoords: b.venue });
        if (venue) occurrences.push({ ...b, resolved: venue });
        else skipped++;
    }

    const queue = writeQueue();
    const seriesLinks = await linkSeries(queue, occurrences.map((o) => seriesOccurrence(o.resolved, {
        venueName: o.activity.venue,
        title: o.activity.title,
        startTime: o.activity.startTime,
        endTime: o.activity.endTime,
        durationEstimated: o.activity.durationEstimated,
        sourceUrl: o.activity.sourceUrl,
        ageRange: o.activity.ageRange,
    })));
    const seriesIds = new Set();
    occurrences.forEach((o, i) => {
        const series = seriesLinks[i];
        if (series) seriesIds.add(series.seriesId);
        queue.set(db.collection("activities").doc(o.activityId), {
            ...o.activity,
            ...venueLocationFields(o.resolved, o.activity.venue),
            seriesId: series ? series.seriesId : null,
            type: series ? "recurring" : "one_time",
            timing: {
                is_all_day: o.activity.isAllDay,
                start_time: o.activity.startTime,
                end_time: o.activity.endTime,
                recurrence: series ? series.rrule : null,
                business_hours: null,
            },
        }, { merge: true });
    });

    cancelledIds.forEach((id) => queue.delete(db.collection("activities").doc(id)));

    // Upcoming events from an earlier run that the source no longer lists (or that this
    // version's rules now leave out) were removed. Only judged on a complete read.
    let removed = 0;
    if (complete) {
        const listed = new Set(occurrences.map((o) => o.activityId));
        const previous = await db.collection("activities")
            .where("source", "==", source)
            .where("tenant", "==", tenant)
            .get();
        for (const doc of previous.docs) {
            if (!listed.has(doc.id) && doc.data().startTime > nowSec) {
                queue.delete(doc.ref);
                removed++;
            }
        }
    }

    // Gemini-scraped activities that this source now covers
    let superseded = 0;
    for (const scraped of SUPERSEDED_SOURCES) {
        const snap = await db.collection("activities").where("source", "==", scraped).get();
        for (const doc of snap.docs) {
            if (isSuperseded(doc.data().sourceUrl || "")) {
                queue.delete(doc.ref);
                superseded++;
            }
        }
    }

    await queue.commit();
    return {
        upserted: occurrences.length,
        venues: new Set(occurrences.map((o) => o.resolved.venueId)).size,
        series: seriesIds.size,
        removed,
        superseded,
        unresolvedVenues: skipped,
    };
}

module.exports = { HORIZON_SEC, partition, storeActivities };
