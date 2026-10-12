// Session lengths. No Firebase dependency, so source parsers can use it directly.

/**
 * Typical lengths of toddler programs, used only when a source lists no end time.
 * A storytime is usually 30 minutes; showing a guessed 1-hour range sent parents to
 * programs that had already ended.
 */
const ESTIMATED_DURATIONS = [
    [/story ?time|lapsit|bounce|rhyme|cuentos/i, 30],
    [/music|movement|sing|song|dance|yoga/i, 45],
];
const DEFAULT_ESTIMATED_DURATION_MIN = 60;

/**
 * End time and length of one session. A listed end time after the start is used as is;
 * otherwise the length is estimated from the title and flagged, so the app can say so.
 * @param {string} title
 * @param {number} startTime - Unix seconds.
 * @param {number|null} [listedEndTime] - Unix seconds from the source, if any.
 * @returns {{endTime: number, durationMinutes: number, durationEstimated: boolean}}
 */
function sessionDuration(title, startTime, listedEndTime) {
    if (Number.isFinite(listedEndTime) && listedEndTime > startTime) {
        return {
            endTime: listedEndTime,
            durationMinutes: Math.round((listedEndTime - startTime) / 60),
            durationEstimated: false,
        };
    }
    const match = ESTIMATED_DURATIONS.find(([pattern]) => pattern.test(String(title || "")));
    const minutes = match ? match[1] : DEFAULT_ESTIMATED_DURATION_MIN;
    return { endTime: startTime + minutes * 60, durationMinutes: minutes, durationEstimated: true };
}

module.exports = { sessionDuration };
