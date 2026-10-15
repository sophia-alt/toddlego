// Plain text from the HTML that event sources put in titles and descriptions.
// No Firebase dependency, so source parsers can use it directly.

/** Decodes HTML entities ("&#038;", "&#8217;", "&rsquo;", "&nbsp;"). */
function decodeEntities(s) {
    return String(s || "")
        .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
        .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
        .replace(/&nbsp;/g, " ")
        .replace(/&quot;/g, "\"")
        .replace(/&apos;/g, "'")
        .replace(/&[lr]squo;/g, "’")
        .replace(/&[lr]dquo;/g, "\"")
        .replace(/&ndash;/g, "–")
        .replace(/&mdash;/g, "—")
        .replace(/&hellip;/g, "…")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&amp;/g, "&");
}

/**
 * Plain-text excerpt of an HTML description, cut at a word boundary. Embedded <style> and
 * <script> blocks and comments (LibCal descriptions carry button styles) are dropped.
 */
function excerpt(html, maxLength = 280) {
    const plain = decodeEntities(String(html || "")
        .replace(/<(style|script)\b[^>]*>[\s\S]*?<\/\1>/gi, " ")
        .replace(/<!--[\s\S]*?-->/g, " ")
        .replace(/<[^>]+>/g, " "))
        .replace(/\s+/g, " ")
        .trim();
    if (plain.length <= maxLength) return plain;
    return plain.slice(0, maxLength).replace(/\s+\S*$/, "") + "…";
}

module.exports = { decodeEntities, excerpt };
