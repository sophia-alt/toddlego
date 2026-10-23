const { onSchedule } = require("firebase-functions/v2/scheduler");
const { createRunBudget } = require("../utils/helpers");
const { partition, storeActivities } = require("../utils/sourceSync");
const { GOOGLE_MAPS_API_KEY } = require("../utils/secrets");
const civicplus = require("../sources/civicplus");

const TIMEOUT_SECONDS = 540;

/**
 * New venues geocoded per run. Venues are found by name after their first geocode, so a run
 * after the first needs few; the cap keeps a run with many new names cheap.
 */
const MAX_GEOCODES_PER_RUN = 150;

/**
 * Syncs one CivicPlus city: upserts its events for young children and families (with venue
 * and weekly series), removes events it has cancelled or no longer lists, and removes
 * Gemini-scraped activities it supersedes.
 * @param {Object} city - Entry from civicplus.CIVICPLUS_CITIES.
 * @param {{dryRun?: boolean, fetchImpl?: Function, apiKey?: string, budget?: Object}} options -
 *   dryRun fetches and maps the events but reads/writes nothing in Firestore.
 */
async function syncCity(city, { dryRun = false, fetchImpl, apiKey, budget } = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const { events, complete } = await civicplus.fetchAllEvents(city, { fetchImpl });
    const { built, cancelledIds, skipped } =
        partition(events.map((event) => civicplus.buildActivity(city, event, { nowSec })), nowSec);

    const summary = { city: city.id, fetched: events.length, complete, mapped: built.length, cancelled: cancelledIds.length, skipped };
    if (dryRun) return { summary, built };

    const stored = await storeActivities({
        source: "civicplus",
        tenant: city.id,
        built,
        cancelledIds,
        complete,
        nowSec,
        isSuperseded: (url) => civicplus.isSupersededUrl(url, city),
        apiKey,
        budget,
    });
    return { summary: { ...summary, ...stored } };
}

/**
 * CivicPlus Events Sync (Daily)
 * Reads city and park district calendars' iCalendar feeds: no LLM. New venues are geocoded
 * once. Daily so cancellations and schedule changes show up quickly; one request per
 * calendar category.
 */
exports.civicplusEventsSync = onSchedule(
    {
        schedule: "0 11 * * *", // Every day at 11:00 UTC, after the Communico sync
        secrets: [GOOGLE_MAPS_API_KEY],
        timeoutSeconds: TIMEOUT_SECONDS,
        memory: "512MiB",
    },
    async (event) => {
        const cities = civicplus.CIVICPLUS_CITIES;
        const budget = createRunBudget({ geminiCalls: 0, geocodeCalls: MAX_GEOCODES_PER_RUN, timeoutSeconds: TIMEOUT_SECONDS });
        const apiKey = GOOGLE_MAPS_API_KEY.value();
        console.log(`🏛️ CivicPlus events sync: ${cities.length} cities`);
        let failures = 0;
        for (const city of cities) {
            if (budget.outOfTime()) {
                console.warn(`⏱️ Stopping early to stay within the ${TIMEOUT_SECONDS}s timeout`);
                break;
            }
            try {
                const { summary } = await syncCity(city, { apiKey, budget });
                console.log(`✅ ${JSON.stringify(summary)}`);
            } catch (e) {
                failures++;
                console.error(`❌ ${city.id}: ${e.message}`);
            }
        }
        console.log(`🎉 CivicPlus events sync complete (${failures} cities failed; ${budget.summary()})`);
        if (failures === cities.length) {
            throw new Error("All CivicPlus cities failed"); // surfaces as a failed run
        }
    },
);

exports.syncCity = syncCity;
