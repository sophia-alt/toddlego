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
const { eventbriteEventsFetcher } = require("./jobs/eventbriteEventsFetcher");
const { rssFeedParser } = require("./jobs/rssFeedParser");
const { cityCalendarScraper } = require("./jobs/cityCalendarScraper");

// Export all functions
exports.dailyLibraryScraper = dailyLibraryScraper;
exports.discoverCaliforniaLibraries = discoverCaliforniaLibraries;
exports.serperDevFetchAndFilterEvents = serperDevFetchAndFilterEvents;
exports.eventbriteEventsFetcher = eventbriteEventsFetcher;
exports.rssFeedParser = rssFeedParser;
exports.cityCalendarScraper = cityCalendarScraper;
