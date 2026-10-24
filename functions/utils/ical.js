// Events from an iCalendar (.ics) feed. Handles what calendar subscription feeds use: folded
// lines, escaped text, local times with a TZID (read as Pacific time), UTC times and all-day
// dates. Recurrence rules are not expanded; feeds that list each session separately need none.
// No Firebase dependency, so source parsers can use it directly.

const { pacificToSec } = require("./pacificTime");

/** "Kids\, Ghosts \; Giggles\nLine 2" to plain text. */
const unescapeText = (s) => String(s || "").replace(/\\([\\;,nN])/g, (_, c) => (c === "n" || c === "N" ? "\n" : c));

/**
 * One iCalendar date or date-time to Unix seconds.
 * @param {string} value - "20261218T183000", "20261218T183000Z" or "20261225".
 * @param {Object} params - Property parameters, e.g. { VALUE: "DATE" }.
 * @returns {{sec: number, allDay: boolean}} sec is NaN when unparseable.
 */
function parseDate(value, params = {}) {
    const m = String(value || "").match(/^(\d{4})(\d\d)(\d\d)(?:T(\d\d)(\d\d)(\d\d)?(Z)?)?$/);
    if (!m) return { sec: NaN, allDay: false };
    const [, y, mo, d, h, mi, s, utc] = m;
    if (h === undefined || params.VALUE === "DATE") {
        return { sec: pacificToSec(`${y}-${mo}-${d} 00:00:00`), allDay: true };
    }
    if (utc) return { sec: Date.UTC(y, mo - 1, d, h, mi, s || 0) / 1000, allDay: false };
    // Local time with a TZID, or floating: the feeds read here are all Bay Area calendars
    return { sec: pacificToSec(`${y}-${mo}-${d} ${h}:${mi}:${s || "00"}`), allDay: false };
}

/**
 * The VEVENTs of a feed.
 * @param {string} text - The .ics body.
 * @returns {Array<Object>} One object per event: { uid, summary, description, location, url,
 *   start, end, allDay, status } with times in Unix seconds (end NaN when absent).
 */
function parseEvents(text) {
    const lines = String(text || "").replace(/\r\n?/g, "\n").replace(/\n[ \t]/g, "").split("\n");
    const events = [];
    let current = null;
    for (const line of lines) {
        if (line === "BEGIN:VEVENT") {
            current = {};
            continue;
        }
        if (line === "END:VEVENT") {
            if (current) events.push(toEvent(current));
            current = null;
            continue;
        }
        if (!current) continue;
        const m = line.match(/^([A-Za-z-]+)((?:;[^:;]+=(?:"[^"]*"|[^:;]*))*):(.*)$/);
        if (!m) continue;
        const params = {};
        for (const p of m[2].split(";").slice(1)) {
            const [k, v = ""] = p.split("=");
            params[k.toUpperCase()] = v.replace(/^"|"$/g, "");
        }
        const name = m[1].toUpperCase();
        if (!(name in current)) current[name] = { value: m[3], params };
    }
    return events;
}

function toEvent(props) {
    const text = (name) => (props[name] ? unescapeText(props[name].value).trim() : "");
    const start = props.DTSTART ? parseDate(props.DTSTART.value, props.DTSTART.params) : { sec: NaN, allDay: false };
    const end = props.DTEND ? parseDate(props.DTEND.value, props.DTEND.params) : { sec: NaN };
    return {
        uid: text("UID"),
        summary: text("SUMMARY"),
        description: text("DESCRIPTION"),
        location: text("LOCATION"),
        url: text("URL"),
        status: text("STATUS").toUpperCase(),
        start: start.sec,
        end: end.sec,
        allDay: start.allDay,
    };
}

module.exports = { parseEvents, parseDate, unescapeText };
