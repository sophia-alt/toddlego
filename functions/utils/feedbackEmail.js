// Turns an in-app feedback doc (`feedback` collection) into an email.
// Pure functions, so the format can be tested without Firestore or SMTP.

/** Inbox that receives feedback; also the Gmail account that sends it. */
const FEEDBACK_INBOX = "plainstudio.dev@gmail.com";

/**
 * Category keys the app sends, and the subject tag for each. Gmail filters on
 * the tag (e.g. subject:"[ToddleGo][Bug]") to label feedback by category.
 */
const CATEGORY_TAGS = {
    bug: "Bug",
    event_info: "Event info",
    feature: "Feature request",
    other: "Other",
};

const SUBJECT_PREVIEW_CHARS = 60;

/** e.g. "[ToddleGo][Bug] Map button does nothing on the Dublin storytime" */
function feedbackSubject(data) {
    const tag = CATEGORY_TAGS[data.category] || CATEGORY_TAGS.other;
    const firstLine = String(data.message || "").trim().split("\n")[0].trim();
    const preview = firstLine.length > SUBJECT_PREVIEW_CHARS
        ? `${firstLine.slice(0, SUBJECT_PREVIEW_CHARS - 1)}…`
        : firstLine;
    return `[ToddleGo][${tag}] ${preview || "(no message)"}`;
}

/** A user-supplied reply address, or null when missing or malformed. */
function replyToAddress(data) {
    const email = String(data.email || "").trim();
    return /^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(email) ? email : null;
}

/** Plain-text body: the message, then the context the app attached, with recent errors last. */
function feedbackBody(data, docId) {
    const context = { ...(data.context || {}) };
    const recentLog = Array.isArray(context.recentLog) ? context.recentLog : [];
    delete context.recentLog;

    const lines = [
        String(data.message || "").trim(),
        "",
        "---",
        `Category: ${CATEGORY_TAGS[data.category] || data.category || "Other"}`,
        `Reply to: ${replyToAddress(data) || "(not given)"}`,
        `Feedback ID: ${docId}`,
        "",
        "Context:",
        JSON.stringify(context, null, 2),
    ];
    if (recentLog.length > 0) {
        lines.push("", `Recent log (${recentLog.length}, oldest first):`);
        for (const entry of recentLog) lines.push(`- ${entry}`);
    }
    return lines.join("\n");
}

module.exports = {
    FEEDBACK_INBOX,
    CATEGORY_TAGS,
    feedbackSubject,
    feedbackBody,
    replyToAddress,
};
