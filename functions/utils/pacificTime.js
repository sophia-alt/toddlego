// Pacific local times from event sources ("2026-10-15 16:00:00") to Unix seconds,
// independent of the process timezone. No Firebase dependency.

/** Offset of Pacific time from UTC (minutes, e.g. -420) at a moment. */
function pacificOffsetMinutes(ms) {
    const parts = Object.fromEntries(new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Los_Angeles",
        hourCycle: "h23",
        year: "numeric", month: "2-digit", day: "2-digit",
        hour: "2-digit", minute: "2-digit", second: "2-digit",
    }).formatToParts(new Date(ms)).map((p) => [p.type, p.value]));
    const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
    return Math.round((asUtc - ms) / 60000);
}

/** "2026-10-15 16:00:00" (Pacific local time) to Unix seconds; NaN when unparseable. */
function pacificToSec(s) {
    const m = String(s || "").match(/^(\d{4})-(\d\d)-(\d\d)[ T](\d\d):(\d\d)(?::(\d\d))?/);
    if (!m) return NaN;
    const wall = Date.UTC(m[1], m[2] - 1, m[3], m[4], m[5], m[6] || 0);
    let ms = wall - pacificOffsetMinutes(wall) * 60000;
    // Near a DST switch the first guess can use the other side's offset
    const offset = pacificOffsetMinutes(ms);
    ms = wall - offset * 60000;
    return Math.floor(ms / 1000);
}

/** The Pacific date at a moment (Unix seconds), e.g. "2026-10-15". */
function pacificDate(sec) {
    return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Los_Angeles" }).format(new Date(sec * 1000));
}

module.exports = { pacificToSec, pacificDate };
