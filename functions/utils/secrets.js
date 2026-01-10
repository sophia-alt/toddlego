// Centralized secret definitions for all Cloud Functions
const { defineSecret } = require("firebase-functions/params");

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");
const GOOGLE_MAPS_API_KEY = defineSecret("GOOGLE_MAPS_API_KEY");
const SERPER_DEV_API_KEY = defineSecret("SERPER_DEV_API_KEY");
const EVENTBRITE_API_KEY = defineSecret("EVENTBRITE_API_KEY");

module.exports = {
    GEMINI_API_KEY,
    GOOGLE_MAPS_API_KEY,
    SERPER_DEV_API_KEY,
    EVENTBRITE_API_KEY,
};
