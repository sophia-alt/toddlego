const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const admin = require("firebase-admin");
const nodemailer = require("nodemailer");
const { FEEDBACK_GMAIL_APP_PASSWORD } = require("../utils/secrets");
const {
    FEEDBACK_INBOX,
    feedbackSubject,
    feedbackBody,
    replyToAddress,
} = require("../utils/feedbackEmail");

/**
 * Emails each in-app feedback report (bug, wrong event info, feature request)
 * to the team inbox, with the context the app attached for debugging.
 * Marks the doc emailedAt on success, or emailError so a failed send is visible
 * in the console; Firestore keeps every report either way.
 */
exports.feedbackEmailer = onDocumentCreated(
    {
        document: "feedback/{feedbackId}",
        secrets: [FEEDBACK_GMAIL_APP_PASSWORD],
        timeoutSeconds: 60,
        memory: "256MiB",
    },
    async (event) => {
        const snap = event.data;
        if (!snap) return;
        const data = snap.data();
        const docId = event.params.feedbackId;

        const transport = nodemailer.createTransport({
            service: "gmail",
            auth: { user: FEEDBACK_INBOX, pass: FEEDBACK_GMAIL_APP_PASSWORD.value() },
        });

        try {
            await transport.sendMail({
                from: `ToddleGo Feedback <${FEEDBACK_INBOX}>`,
                to: FEEDBACK_INBOX,
                replyTo: replyToAddress(data) || undefined,
                subject: feedbackSubject(data),
                text: feedbackBody(data, docId),
            });
            await snap.ref.update({ emailedAt: admin.firestore.FieldValue.serverTimestamp() });
            console.log(`[feedbackEmailer] Sent ${docId} (${data.category})`);
        } catch (error) {
            console.error(`[feedbackEmailer] Failed to send ${docId}:`, error.message);
            await snap.ref.update({ emailError: String(error.message || error) });
        }
    },
);
