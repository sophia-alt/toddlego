const { onSchedule } = require("firebase-functions/v2/scheduler");
const { partition, storeActivities } = require("../utils/sourceSync");
const libcal = require("../sources/libcal");

const TIMEOUT_SECONDS = 900;

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
    const { built, cancelledIds, skipped } =
        partition(events.map((event) => libcal.buildActivity(library, event, { nowSec })), nowSec);

    const summary = { tenant, fetched: events.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    const stored = await storeActivities({
        source: "libcal",
        tenant,
        built,
        cancelledIds,
        complete,
        nowSec,
        isSuperseded: (url) => libcal.isSupersededUrl(url, library),
    });
    return { summary: { ...summary, ...stored } };
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
