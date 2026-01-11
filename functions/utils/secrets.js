// Centralized secret definitions for all Cloud Functions
const { defineSecret } = require("firebase-functions/params");

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");
const GOOGLE_MAPS_API_KEY = defineSecret("GOOGLE_MAPS_API_KEY");
const SERPER_DEV_API_KEY = defineSecret("SERPER_DEV_API_KEY");
// Note: EVENTBRITE_API_KEY removed - Eventbrite deprecated public search API in 2019
// No replacement API endpoint available.

module.exports = {
    GEMINI_API_KEY,
    GOOGLE_MAPS_API_KEY,
    SERPER_DEV_API_KEY,
};
