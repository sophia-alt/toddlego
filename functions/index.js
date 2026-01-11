// Main index file - exports all Cloud Functions from separate job files

// Initialize Firebase Admin (only once)
const admin = require("firebase-admin");
if (!admin.apps.length) {
    admin.initializeApp();
}

// Import and re-export all scheduled functions
const { dailyLibraryScraper } = require("./jobs/dailyLibraryScraper");
const { discoverCaliforniaLibraries } = require("./jobs/discoverCaliforniaLibraries");
const { serperDevFetchAndFilterEvents } = require("./jobs/serperDevFetchAndFilterEvents");
const { rssFeedParser } = require("./jobs/rssFeedParser");
const { cityCalendarScraper } = require("./jobs/cityCalendarScraper");
// Note: eventbriteEventsFetcher removed - Eventbrite deprecated public search API in 2019
// No replacement API endpoint available. Using alternative data sources instead.

// Export all functions
exports.dailyLibraryScraper = dailyLibraryScraper;
exports.discoverCaliforniaLibraries = discoverCaliforniaLibraries;
exports.serperDevFetchAndFilterEvents = serperDevFetchAndFilterEvents;
exports.rssFeedParser = rssFeedParser;
exports.cityCalendarScraper = cityCalendarScraper;
