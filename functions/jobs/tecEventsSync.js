const { onSchedule } = require("firebase-functions/v2/scheduler");
const { db } = require("../utils/helpers");
const { resolveVenue, venueLocationFields } = require("../utils/venues");
const { linkSeries, seriesOccurrence } = require("../utils/series");
const { writeQueue } = require("../utils/writeQueue");
const tec = require("../sources/tec");

const TIMEOUT_SECONDS = 900;

/** Only events in the next 60 days are stored; later runs pick up later ones. */
const HORIZON_SEC = 60 * 24 * 60 * 60;

/** Activities other jobs scraped with Gemini; replaced by the API for covered sites. */
const SUPERSEDED_SOURCES = ["city_calendar", "web_scrape"];

/**
 * Syncs one TEC site: upserts its young-children events (with venue and weekly series),
 * removes events it has cancelled or no longer lists, and removes Gemini-scraped
 * activities it supersedes.
 * @param {Object} site - Entry from tec.TEC_SITES.
 * @param {{dryRun?: boolean, fetchImpl?: Function}} options - dryRun fetches and maps the
 *   events but reads/writes nothing in Firestore.
 */
async function syncSite(site, { dryRun = false, fetchImpl } = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const { events, complete } = await tec.fetchAllEvents(site, {
        fromSec: nowSec,
        toSec: nowSec + HORIZON_SEC,
        fetchImpl,
    });

    const built = [];
    const cancelledIds = [];
    let skipped = 0;
    for (const event of events) {
        const b = tec.buildActivity(site, event, { nowSec });
        if (b && b.cancelled) {
            cancelledIds.push(b.activityId);
        } else if (!b || b.activity.endTime < nowSec) {
            skipped++;
        } else {
            built.push(b);
        }
    }

    const summary = { site: site.id, fetched: events.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    // One venue per site, from its fixed coordinates, so this never geocodes
    const venue = await resolveVenue(site.venue.name, { knownCoords: site.venue });
    if (!venue) throw new Error(`${site.id}: invalid venue coordinates`);

    const queue = writeQueue();
    const seriesLinks = await linkSeries(queue, built.map((b) => seriesOccurrence(venue, {
        venueName: b.activity.venue,
        title: b.activity.title,
        startTime: b.activity.startTime,
        endTime: b.activity.endTime,
        durationEstimated: b.activity.durationEstimated,
        sourceUrl: b.activity.sourceUrl,
        ageRange: b.activity.ageRange,
    })));
    const seriesIds = new Set();
    built.forEach((b, i) => {
        const series = seriesLinks[i];
        if (series) seriesIds.add(series.seriesId);
        queue.set(db.collection("activities").doc(b.activityId), {
            ...b.activity,
            ...venueLocationFields(venue, b.activity.venue),
            seriesId: series ? series.seriesId : null,
            type: series ? "recurring" : "one_time",
            timing: {
                is_all_day: b.activity.isAllDay,
                start_time: b.activity.startTime,
                end_time: b.activity.endTime,
                recurrence: series ? series.rrule : null,
                business_hours: null,
            },
        }, { merge: true });
    });

    cancelledIds.forEach((id) => queue.delete(db.collection("activities").doc(id)));

    // Upcoming events from an earlier run that the site no longer lists (or that this
    // version's rules now leave out) were removed. Only judged on a complete read.
    let removed = 0;
    if (complete) {
        const listed = new Set(built.map((b) => b.activityId));
        const previous = await db.collection("activities")
            .where("source", "==", "tec")
            .where("tenant", "==", site.id)
            .get();
        for (const doc of previous.docs) {
            if (!listed.has(doc.id) && doc.data().startTime > nowSec) {
                queue.delete(doc.ref);
                removed++;
            }
        }
    }

    // Gemini-scraped activities from this site that the API now covers
    let superseded = 0;
    for (const source of SUPERSEDED_SOURCES) {
        const snap = await db.collection("activities").where("source", "==", source).get();
        for (const doc of snap.docs) {
            if (tec.isSupersededUrl(doc.data().sourceUrl || "", site)) {
                queue.delete(doc.ref);
                superseded++;
            }
        }
    }

    await queue.commit();
    return {
        summary: { ...summary, upserted: built.length, series: seriesIds.size, removed, superseded },
    };
}

/**
 * TEC Events Sync (Daily)
 * Reads museums' and parks' The Events Calendar REST APIs: no LLM, no geocoding.
 * Daily so cancellations and schedule changes show up quickly; each run is a few
 * requests per site, spaced by the site's crawl delay.
 */
exports.tecEventsSync = onSchedule(
    {
        schedule: "30 9 * * *", // Every day at 09:30 UTC, after the BiblioCommons sync
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "256MiB",
    },
    async (event) => {
        console.log(`🎪 TEC events sync: ${tec.TEC_SITES.length} sites`);
        let failures = 0;
        for (const site of tec.TEC_SITES) {
            try {
                const { summary } = await syncSite(site);
                console.log(`✅ ${JSON.stringify(summary)}`);
            } catch (e) {
                failures++;
                console.error(`❌ ${site.id}: ${e.message}`);
            }
        }
        console.log(`🎉 TEC events sync complete (${failures} sites failed)`);
        if (failures === tec.TEC_SITES.length) {
            throw new Error("All TEC sites failed"); // surfaces as a failed run
        }
    },
);

exports.syncSite = syncSite;
