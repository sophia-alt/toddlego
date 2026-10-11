#!/usr/bin/env node
/**
 * One-off setup helper: prints a library's BiblioCommons audience IDs for children 0-5,
 * to paste into BAY_AREA_TENANTS in sources/bibliocommons.js.
 * Makes one request to the library's events page; the sync job itself only reads RSS.
 *
 * Usage: node discover-bibliocommons-audiences.js <tenant> [<tenant> ...]
 *   e.g. node discover-bibliocommons-audiences.js aclibrary sjpl
 */
const { isYoungChildAudience, USER_AGENT } = require("./sources/bibliocommons");

/** Extracts the JSON object that follows `"key":` in the page's embedded state. */
function extractObject(html, key) {
    const start = html.indexOf(`"${key}":{`);
    if (start < 0) return null;
    let i = html.indexOf("{", start);
    const from = i;
    let depth = 0;
    for (; i < html.length; i++) {
        if (html[i] === "{") depth++;
        else if (html[i] === "}" && --depth === 0) break;
    }
    return JSON.parse(html.slice(from, i + 1));
}

async function discover(tenant) {
    const res = await fetch(`https://${tenant}.bibliocommons.com/v2/events`, {
        headers: { "User-Agent": USER_AGENT },
        signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const audiences = extractObject(await res.text(), "eventAudiences");
    if (!audiences) throw new Error("no eventAudiences in page state");
    const all = Object.values(audiences);
    const young = all.filter((a) => isYoungChildAudience(a.name));
    return { all: all.map((a) => a.name), young };
}

(async () => {
    const tenants = process.argv.slice(2);
    if (!tenants.length) {
        console.error("Usage: node discover-bibliocommons-audiences.js <tenant> [<tenant> ...]");
        process.exit(1);
    }
    for (const tenant of tenants) {
        try {
            const { all, young } = await discover(tenant);
            console.log(`// ${tenant}: all audiences = ${all.join(" | ")}`);
            console.log(`// matched: ${young.map((a) => a.name).join(" | ") || "(none)"}`);
            console.log(`{ tenant: "${tenant}", audienceIds: ${JSON.stringify(young.map((a) => a.id))} },`);
        } catch (e) {
            console.log(`// ${tenant}: failed (${e.message}) - leave audienceIds empty`);
            console.log(`{ tenant: "${tenant}", audienceIds: [] },`);
        }
    }
})();
