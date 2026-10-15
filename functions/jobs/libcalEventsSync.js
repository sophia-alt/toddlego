const { onSchedule } = require("firebase-functions/v2/scheduler");
const { db } = require("../utils/helpers");
const { resolveVenue, venueLocationFields } = require("../utils/venues");
const { linkSeries, seriesOccurrence } = require("../utils/series");
const { writeQueue } = require("../utils/writeQueue");
const libcal = require("../sources/libcal");

const TIMEOUT_SECONDS = 900;

/** Only events in the next 60 days are stored; later runs pick up later ones. */
const HORIZON_SEC = 60 * 24 * 60 * 60;

/** Activities other jobs scraped with Gemini; replaced by LibCal for covered libraries. */
const SUPERSEDED_SOURCES = ["city_calendar", "web_scrape"];

/**
 * Syncs one LibCal library: upserts its young-children events (with venue and weekly
 * series), removes events it has cancelled or no longer lists, and removes Gemini-scraped
 * activities it supersedes.
 * @param {Object} library - Entry from libcal.LIBCAL_LIBRARIES.
 * @param {{dryRun?: boolean, fetchImpl?: Function}} options - dryRun fetches and maps the
 *   events but reads/writes nothing in Firestore.
 */
async function syncLibrary(library, { dryRun = false, fetchImpl } = {}) {
    const { tenant } = library;
    const nowSec = Math.floor(Date.now() / 1000);
    const { events, complete } = await libcal.fetchAllEvents(library, { fetchImpl });

    const built = [];
    const cancelledIds = [];
    let skipped = 0;
    for (const event of events) {
        const b = libcal.buildActivity(library, event, { nowSec });
        if (b && b.cancelled) {
            cancelledIds.push(b.activityId);
        } else if (!b || b.activity.endTime < nowSec || b.activity.startTime > nowSec + HORIZON_SEC) {
            skipped++;
        } else {
            built.push(b);
        }
    }

    const summary = { tenant, fetched: events.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    // Venues: branch and off-site coordinates come from the config, so this never geocodes
    const memo = new Map();
    const occurrences = [];
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

    // Upcoming events from an earlier run that the library no longer lists (or that this
    // version's rules now leave out) were removed. Only judged on a complete read.
    let removed = 0;
    if (complete) {
        const listed = new Set(occurrences.map((o) => o.activityId));
        const previous = await db.collection("activities")
            .where("source", "==", "libcal")
            .where("tenant", "==", tenant)
            .get();
        for (const doc of previous.docs) {
            if (!listed.has(doc.id) && doc.data().startTime > nowSec) {
                queue.delete(doc.ref);
                removed++;
            }
        }
    }

    // Gemini-scraped activities for this library that LibCal now covers
    let superseded = 0;
    for (const source of SUPERSEDED_SOURCES) {
        const snap = await db.collection("activities").where("source", "==", source).get();
        for (const doc of snap.docs) {
            if (libcal.isSupersededUrl(doc.data().sourceUrl || "", library)) {
                queue.delete(doc.ref);
                superseded++;
            }
        }
    }

    await queue.commit();
    return {
        summary: {
            ...summary,
            upserted: occurrences.length,
            venues: new Set(occurrences.map((o) => o.resolved.venueId)).size,
            series: seriesIds.size,
            removed,
            superseded,
            skipped,
        },
    };
}

/**
 * LibCal Events Sync (Daily)
 * Reads Bay Area libraries' LibCal calendars: no LLM, no geocoding.
 * Daily so cancellations and schedule changes show up quickly. Libraries are read one at a
 * time (they share libcal.com), a few pages each with LibCal's 10-second crawl delay.
 */
exports.libcalEventsSync = onSchedule(
    {
        schedule: "0 10 * * *", // Every day at 10:00 UTC, after the BiblioCommons and TEC syncs
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        const libraries = libcal.LIBCAL_LIBRARIES;
        console.log(`📅 LibCal events sync: ${libraries.length} libraries`);
        let failures = 0;
        for (const library of libraries) {
            try {
                const { summary } = await syncLibrary(library);
                console.log(`✅ ${JSON.stringify(summary)}`);
            } catch (e) {
                failures++;
                console.error(`❌ ${library.tenant}: ${e.message}`);
            }
        }
        console.log(`🎉 LibCal events sync complete (${failures} libraries failed)`);
        if (failures === libraries.length) {
            throw new Error("All LibCal libraries failed"); // surfaces as a failed run
        }
    },
);

exports.syncLibrary = syncLibrary;
