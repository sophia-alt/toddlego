// Main index file - exports all Cloud Functions from separate job files

// Scraped event times have no UTC offset (e.g. "2026-10-12T10:30:00") and are
// local Bay Area times. Cloud Functions run in UTC, so pin the process timezone
// before any Date parsing happens; otherwise every event is shifted by 7-8 hours.
process.env.TZ = "America/Los_Angeles";

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
const { bibliocommonsRssSync } = require("./jobs/bibliocommonsRssSync");
// Note: eventbriteEventsFetcher removed - Eventbrite deprecated public search API in 2019
// No replacement API endpoint available. Using alternative data sources instead.

// Export all functions
exports.dailyLibraryScraper = dailyLibraryScraper;
exports.discoverCaliforniaLibraries = discoverCaliforniaLibraries;
exports.serperDevFetchAndFilterEvents = serperDevFetchAndFilterEvents;
exports.rssFeedParser = rssFeedParser;
exports.cityCalendarScraper = cityCalendarScraper;
exports.bibliocommonsRssSync = bibliocommonsRssSync;
