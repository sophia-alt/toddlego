const { onSchedule } = require("firebase-functions/v2/scheduler");
const { db } = require("../utils/helpers");
const { resolveVenue, venueLocationFields } = require("../utils/venues");
const { linkSeries, seriesOccurrence } = require("../utils/series");
const bc = require("../sources/bibliocommons");

const TIMEOUT_SECONDS = 1800;

/**
 * Only events in the next 60 days are stored; later ones are picked up by later daily runs.
 * Libraries list ~4 months ahead (6,000+ Bay Area events), more than the app needs.
 */
const HORIZON_SEC = 60 * 24 * 60 * 60;

/** Activities other jobs scraped with Gemini; replaced by the feed for covered libraries. */
const SUPERSEDED_SOURCES = ["city_calendar", "web_scrape"];

/**
 * Collects writes so they can be committed in chunks below Firestore's 500-op batch limit.
 * Has the batch methods linkSeries() uses.
 */
function writeQueue() {
    const ops = [];
    return {
        set: (ref, data, options) => ops.push((b) => b.set(ref, data, options)),
        delete: (ref) => ops.push((b) => b.delete(ref)),
        size: () => ops.length,
        async commit() {
            for (let i = 0; i < ops.length; i += 450) {
                const batch = db.batch();
                ops.slice(i, i + 450).forEach((op) => op(batch));
                await batch.commit();
            }
        },
    };
}

/**
 * Syncs one library: upserts its young-children events (with venue and weekly series),
 * removes events it has cancelled or no longer lists, and removes Gemini-scraped
 * activities it supersedes.
 * @param {{tenant: string, audienceIds: string[], websiteHosts: string[]}} config
 * @param {{dryRun?: boolean}} options - dryRun fetches and maps the feed but reads/writes
 *   nothing in Firestore.
 */
async function syncTenant(config, { dryRun = false } = {}) {
    const { tenant } = config;
    const nowSec = Math.floor(Date.now() / 1000);
    const { libraryName, items, complete } = await bc.fetchAllItems(tenant, { audienceIds: config.audienceIds });

    const built = [];
    const cancelledIds = [];
    let skipped = 0;
    for (const item of items) {
        if (item.isCancelled) {
            cancelledIds.push(bc.activityId(tenant, item.id));
            continue;
        }
        const b = bc.buildActivity(tenant, libraryName, item, { nowSec });
        if (!b || b.activity.endTime < nowSec || b.activity.startTime > nowSec + HORIZON_SEC) {
            skipped++;
            continue;
        }
        built.push(b);
    }

    const summary = { tenant, libraryName, fetched: items.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    // Venues: the feed's branch coordinates are used, so this never geocodes
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

    // Upcoming events from an earlier run that the library no longer lists were removed by it.
    // Only judged on a complete read of the feed, so a capped read never deletes real events.
    let removed = 0;
    if (complete) {
        const listed = new Set(occurrences.map((o) => o.activityId));
        const previous = await db.collection("activities")
            .where("source", "==", "bibliocommons")
            .where("tenant", "==", tenant)
            .get();
        for (const doc of previous.docs) {
            if (!listed.has(doc.id) && doc.data().startTime > nowSec) {
                queue.delete(doc.ref);
                removed++;
            }
        }
    }

    // Gemini-scraped activities for this library that the feed now covers
    let superseded = 0;
    for (const source of SUPERSEDED_SOURCES) {
        const snap = await db.collection("activities").where("source", "==", source).get();
        for (const doc of snap.docs) {
            if (bc.isSupersededUrl(doc.data().sourceUrl || "", config)) {
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
 * BiblioCommons RSS Sync (Daily)
 * Reads Bay Area libraries' official events RSS feeds: no LLM, no geocoding.
 * Daily so cancellations and schedule changes show up quickly; each run is a few
 * lightweight feed requests per library.
 */
exports.bibliocommonsRssSync = onSchedule(
    {
        schedule: "0 9 * * *", // Every day at 09:00 UTC (early morning in California)
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        console.log(`📚 BiblioCommons RSS sync: ${bc.BAY_AREA_TENANTS.length} libraries`);
        let failures = 0;
        for (const config of bc.BAY_AREA_TENANTS) {
            try {
                const { summary } = await syncTenant(config);
                console.log(`✅ ${JSON.stringify(summary)}`);
            } catch (e) {
                failures++;
                console.error(`❌ ${config.tenant}: ${e.message}`);
            }
        }
        console.log(`🎉 BiblioCommons RSS sync complete (${failures} libraries failed)`);
        if (failures === bc.BAY_AREA_TENANTS.length) {
            throw new Error("All BiblioCommons feeds failed"); // surfaces as a failed run
        }
    },
);

exports.syncTenant = syncTenant;
