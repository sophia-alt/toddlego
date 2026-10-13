// Offline tests for the feedback email format (utils/feedbackEmail.js).
const test = require("node:test");
const assert = require("node:assert");
const {
    feedbackSubject,
    feedbackBody,
    replyToAddress,
} = require("../utils/feedbackEmail");

const report = {
    category: "bug",
    message: "Map button does nothing\nTapped it twice on the Dublin storytime.",
    email: "parent@example.com",
    context: {
        appVersion: "1.0.0+1",
        screen: "activity_detail",
        activity: { id: "abc123", title: "Baby Bounce" },
        recentLog: ["10:30:01 Could not launch https://maps.google.com"],
    },
};

test("subject tags the category and previews the first line", () => {
    assert.strictEqual(feedbackSubject(report), "[ToddleGo][Bug] Map button does nothing");
    assert.strictEqual(
        feedbackSubject({ category: "feature", message: "Add a nap-time filter" }),
        "[ToddleGo][Feature request] Add a nap-time filter",
    );
    assert.strictEqual(feedbackSubject({ category: "?", message: "" }), "[ToddleGo][Other] (no message)");
    const long = feedbackSubject({ category: "other", message: "x".repeat(100) });
    assert.strictEqual(long, `[ToddleGo][Other] ${"x".repeat(59)}…`);
});

test("reply-to uses the user's email only when it looks valid", () => {
    assert.strictEqual(replyToAddress(report), "parent@example.com");
    assert.strictEqual(replyToAddress({ email: "not an email" }), null);
    assert.strictEqual(replyToAddress({}), null);
});

test("body has the message, then context, then the recent log", () => {
    const body = feedbackBody(report, "fb1");
    assert.ok(body.startsWith("Map button does nothing\nTapped it twice"));
    assert.match(body, /Category: Bug/);
    assert.match(body, /Feedback ID: fb1/);
    assert.match(body, /"screen": "activity_detail"/);
    assert.match(body, /"id": "abc123"/);
    assert.ok(!body.includes('"recentLog"'), "log is listed separately, not in the JSON");
    assert.match(body, /Recent log \(1, oldest first\):\n- 10:30:01 Could not launch/);
});
