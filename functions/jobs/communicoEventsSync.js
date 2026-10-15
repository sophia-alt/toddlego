const { onSchedule } = require("firebase-functions/v2/scheduler");
const { partition, storeActivities } = require("../utils/sourceSync");
const communico = require("../sources/communico");

const TIMEOUT_SECONDS = 540;

/**
 * Syncs one Communico library: upserts its young-children events (with venue and weekly
 * series), removes events it has cancelled or no longer lists, and removes Gemini-scraped
 * activities it supersedes.
 * @param {Object} library - Entry from communico.COMMUNICO_LIBRARIES.
 * @param {{dryRun?: boolean, fetchImpl?: Function}} options - dryRun fetches and maps the
 *   events but reads/writes nothing in Firestore.
 */
async function syncLibrary(library, { dryRun = false, fetchImpl } = {}) {
    const { tenant } = library;
    const nowSec = Math.floor(Date.now() / 1000);
    const { events, complete } = await communico.fetchAllEvents(library, { nowSec, fetchImpl });
    const { built, cancelledIds, skipped } =
        partition(events.map((event) => communico.buildActivity(library, event, { nowSec })), nowSec);

    const summary = { tenant, fetched: events.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    const stored = await storeActivities({
        source: "communico",
        tenant,
        built,
        cancelledIds,
        complete,
        nowSec,
        isSuperseded: (url) => communico.isSupersededUrl(url, library),
    });
    return { summary: { ...summary, ...stored } };
}

/**
 * Communico Events Sync (Daily)
 * Reads Bay Area libraries' Communico calendars: no LLM, no geocoding.
 * Daily so cancellations and schedule changes show up quickly; one request per library.
 */
exports.communicoEventsSync = onSchedule(
    {
        schedule: "30 10 * * *", // Every day at 10:30 UTC, after the LibCal sync
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        const libraries = communico.COMMUNICO_LIBRARIES;
        console.log(`📅 Communico events sync: ${libraries.length} libraries`);
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
        console.log(`🎉 Communico events sync complete (${failures} libraries failed)`);
        if (failures === libraries.length) {
            throw new Error("All Communico libraries failed"); // surfaces as a failed run
        }
    },
);

exports.syncLibrary = syncLibrary;
