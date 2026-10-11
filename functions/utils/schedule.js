/**
 * Schedules as Gemini returns them: one entry per program, with a weekly recurrence rule
 * and/or the specific dates a page lists, instead of one entry per session.
 *
 * A weekly storytime listed for eight Wednesdays is one entry instead of eight (fewer output
 * tokens), and pages that only say "every Wednesday at 10:30" now yield sessions at all.
 * expandToSessions() turns an entry back into dated sessions, so IDs, venues, series
 * inference and writes work exactly as for per-session extraction.
 */

const { toDateKey } = require("./helpers");

/**
 * Bump when the extraction format changes; cached parses from an older version are re-parsed.
 * 2: programs with `recurrence`/`dates` instead of one `isoDate` per session.
 */
const PARSE_VERSION = 2;

/** Sessions generated from a recurrence rule are only this far ahead; pages are re-read weekly. */
const RECURRENCE_HORIZON_DAYS = 28;

/** Guard against a bad rule or date list flooding the database. */
const MAX_SESSIONS_PER_PROGRAM = 60;

const WEEKDAYS = ["SU", "MO", "TU", "WE", "TH", "FR", "SA"];
const WEEKDAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/** Days since epoch for "YYYY-MM-DD" (DST-independent), or NaN if not a valid date. */
function dayNumber(dateKey) {
    if (!DATE_RE.test(String(dateKey))) return NaN;
    const [y, m, d] = dateKey.split("-").map(Number);
    const t = Date.UTC(y, m - 1, d);
    const back = new Date(t);
    if (back.getUTCFullYear() !== y || back.getUTCMonth() !== m - 1 || back.getUTCDate() !== d) return NaN;
    return t / 86400000;
}

const dateKeyOf = (day) => new Date(day * 86400000).toISOString().slice(0, 10);
const weekdayOf = (day) => new Date(day * 86400000).getUTCDay();

/** "HH:mm" (24-hour, zero-padded) or null. Accepts "9:30". */
function normalizeTime(value) {
    const m = String(value ?? "").trim().match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const h = Number(m[1]);
    const min = Number(m[2]);
    if (h > 23 || min > 59) return null;
    return `${String(h).padStart(2, "0")}:${m[2]}`;
}

/**
 * Prompt section describing the schedule fields, shared by the page scrapers.
 * @param {Date} [now]
 */
function scheduleInstructions(now = new Date()) {
    const today = toDateKey(now);
    return `
        ### SCHEDULE (CRITICAL)
        - Today is ${today} (${WEEKDAY_NAMES[now.getDay()]}). All times are local Pacific time.
        - Return ONE entry per program. A program that repeats weekly (e.g. "Every Wednesday at 10:30", or the same
          title, branch, weekday and time listed on several dates) is a single entry with "recurrence" - do NOT
          list each session separately.
        - Only use "recurrence" when the page states or clearly shows a weekly (or every-other-week) pattern. Never
          invent one. Monthly or irregular programs have no recurrence: list their dates instead.
        - "dates": the specific dates (YYYY-MM-DD) the page lists for the program - every date of a one-time or
          irregular program, and any dated sessions of a recurring one. Resolve relative dates ("tomorrow",
          "next Wednesday") from today's date. Use the year ${now.getFullYear()} unless the page says otherwise.
        - "exceptDates": dates the page says a recurring program does NOT run (holidays, cancellations).
        - "startTime" / "endTime": 24-hour "HH:mm". If the start time is not given, leave the program out.
        - Every entry needs at least one date in "dates" or a "recurrence". If the page gives neither (no
          dates and no weekly pattern), leave the program out.
        - If a program runs at different times on different days, return one entry per start time.

        Schedule fields of each entry:
        "startTime": "HH:mm",
        "endTime": "HH:mm or null",
        "dates": ["YYYY-MM-DD"],
        "recurrence": null or {
            "byDay": ["MO" | "TU" | "WE" | "TH" | "FR" | "SA" | "SU"],
            "intervalWeeks": 1 or 2,
            "startDate": "YYYY-MM-DD or null (first session, if the page says)",
            "endDate": "YYYY-MM-DD or null (last session / end of season, if the page says)"
        },
        "exceptDates": ["YYYY-MM-DD"]
    `;
}

/**
 * Dated sessions of one extracted program, as per-session entries with `isoDate` and
 * `endTime` ("YYYY-MM-DDTHH:mm:00"), which the scrapers already consume.
 *
 * Entries in the previous per-session format (with `isoDate`) pass through unchanged, so a
 * parse cached before PARSE_VERSION 2 still works.
 *
 * @param {Object} program - One entry of Gemini's "events" array.
 * @param {{today?: string, horizonDays?: number}} [options] - today as "YYYY-MM-DD" (local).
 * @returns {Object[]} Sessions, each the program's fields plus isoDate, endTime and
 *   scheduleSource ("listed" for dates on the page, "recurrence" for dates from the rule).
 */
function expandToSessions(program, { today = toDateKey(new Date()), horizonDays = RECURRENCE_HORIZON_DAYS } = {}) {
    if (!program || typeof program !== "object") return [];
    if (program.isoDate) return [{ ...program, scheduleSource: program.scheduleSource || "listed" }];

    const startTime = normalizeTime(program.startTime);
    if (!startTime) return [];
    const endTime = normalizeTime(program.endTime);

    const sessions = new Map(); // dateKey -> scheduleSource
    for (const d of Array.isArray(program.dates) ? program.dates : []) {
        if (!Number.isNaN(dayNumber(d))) sessions.set(d, "listed");
    }

    for (const d of recurrenceDates(program.recurrence, today, horizonDays)) {
        if (!sessions.has(d)) sessions.set(d, "recurrence");
    }

    const except = new Set(Array.isArray(program.exceptDates) ? program.exceptDates : []);
    const { recurrence, dates, exceptDates, ...fields } = program;
    return [...sessions]
        .filter(([d]) => !except.has(d))
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .slice(0, MAX_SESSIONS_PER_PROGRAM)
        .map(([d, scheduleSource]) => ({
            ...fields,
            startTime,
            isoDate: `${d}T${startTime}:00`,
            endTime: endTime && endTime > startTime ? `${d}T${endTime}:00` : null,
            scheduleSource,
        }));
}

/** Dates a weekly rule produces from today (or its start) up to the horizon (or its end). */
function recurrenceDates(recurrence, today, horizonDays) {
    if (!recurrence || typeof recurrence !== "object") return [];
    const byDay = (Array.isArray(recurrence.byDay) ? recurrence.byDay : [])
        .map((d) => WEEKDAYS.indexOf(String(d).toUpperCase().slice(0, 2)))
        .filter((i) => i >= 0);
    if (byDay.length === 0) return [];

    const interval = Number(recurrence.intervalWeeks ?? 1);
    const ruleStart = dayNumber(recurrence.startDate);
    // Every other week needs the first session to know which weeks; without it, skip the rule
    if (interval === 2 && Number.isNaN(ruleStart)) return [];
    if (interval !== 1 && interval !== 2) return [];

    const todayDay = dayNumber(today);
    const ruleEnd = dayNumber(recurrence.endDate);
    const from = Number.isNaN(ruleStart) ? todayDay : Math.max(todayDay, ruleStart);
    const to = Math.min(todayDay + horizonDays - 1, Number.isNaN(ruleEnd) ? Infinity : ruleEnd);

    const out = [];
    for (let day = from; day <= to; day++) {
        const wd = weekdayOf(day);
        if (!byDay.includes(wd)) continue;
        if (interval === 2) {
            // First session of this weekday on or after the rule's start anchors the fortnights
            const anchor = ruleStart + ((wd - weekdayOf(ruleStart) + 7) % 7);
            if ((day - anchor) % 14 !== 0) continue;
        }
        out.push(dateKeyOf(day));
    }
    return out;
}

module.exports = {
    PARSE_VERSION,
    RECURRENCE_HORIZON_DAYS,
    scheduleInstructions,
    expandToSessions,
    normalizeTime,
};
