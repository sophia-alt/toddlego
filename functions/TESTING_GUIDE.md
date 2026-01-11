# Testing Guide for Toddlego Cloud Functions

This guide covers how to test all Cloud Functions locally and in production.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Testing](#local-testing)
3. [Production Testing](#production-testing)
4. [Manual Function Invocation](#manual-function-invocation)
5. [Verifying Results](#verifying-results)
6. [Monitoring & Debugging](#monitoring--debugging)
7. [Testing Individual Functions](#testing-individual-functions)

---

## Prerequisites

### 1. Install Firebase CLI
```bash
npm install -g firebase-tools
firebase login
```

### 2. Install Function Dependencies
```bash
cd apps/toddlego/functions
npm install
```

### 3. Set Up Firebase Secrets (for production testing)
```bash
# Set secrets (you'll be prompted to enter values)
firebase functions:secrets:set GEMINI_API_KEY
firebase functions:secrets:set GOOGLE_MAPS_API_KEY
firebase functions:secrets:set SERPER_DEV_API_KEY

# Verify secrets are set
firebase functions:secrets:access
```

### 4. Create `.env` file for Local Testing (Optional)
Create `functions/.env` file for local emulator (gitignored):
```env
GEMINI_API_KEY=your_gemini_api_key
GOOGLE_MAPS_API_KEY=your_google_maps_api_key
SERPER_DEV_API_KEY=your_serper_dev_api_key
```

---

## Local Testing

### Method 1: Firebase Emulator Suite (Recommended)

#### Start the Emulator
```bash
cd apps/toddlego
firebase emulators:start --only functions,firestore
```

This will:
- Start Functions emulator on `http://localhost:5001`
- Start Firestore emulator on `http://localhost:8080`
- Use your actual Firebase project data (read-only)

#### Test Functions via Emulator UI
1. Open Firebase Emulator UI: `http://localhost:4000`
2. Navigate to "Functions" tab
3. Select a function and click "Test" or "Run"
4. View logs and execution results

#### Test Functions via CLI
In a separate terminal:
```bash
# Test specific function
curl -X POST http://localhost:5001/toddlego-81c25/us-central1/dailyLibraryScraper \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Method 2: Direct Node.js Execution (Quick Testing)

For quick testing of helper functions or logic, you can create test scripts:

```bash
# Create a test file
cd functions
node test-helpers.js
```

Example test script (`functions/test-helpers.js`):
```javascript
const { getDynamicCoordinates, generateEventId } = require('./utils/helpers');

async function test() {
  // Test geocoding
  const coords = await getDynamicCoordinates("Oakland Public Library", "Oakland, CA");
  console.log("Coordinates:", coords);
  
  // Test event ID generation
  const id = generateEventId("Storytime", "Oakland Library", "2025-01-15");
  console.log("Event ID:", id);
}

test().catch(console.error);
```

---

## Production Testing

### Method 1: Firebase Console (Easiest)

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select your project: `toddlego-81c25`
3. Navigate to **Functions** → **Cloud Functions**
4. Click on a function name
5. Click **"Test"** tab
6. Click **"Test the function"** button
7. View logs and execution time

### Method 2: Manual Invocation via CLI

```bash
# Test a scheduled function manually
cd apps/toddlego

# Daily Library Scraper
firebase functions:call dailyLibraryScraper

# Discover California Libraries
firebase functions:call discoverCaliforniaLibraries

# Serper.dev Events Fetcher
firebase functions:call serperDevFetchAndFilterEvents

# RSS Feed Parser
firebase functions:call rssFeedParser

# City Calendar Scraper
firebase functions:call cityCalendarScraper
```

### Method 3: HTTP Trigger (if functions were HTTP)

If you need to test via HTTP, you can temporarily convert a scheduled function to HTTP:

```javascript
// In the function file, temporarily change:
exports.testFunction = onRequest({ cors: true }, async (req, res) => {
  // Your function logic here
  res.json({ success: true, message: "Function executed" });
});
```

Then call:
```bash
curl https://us-central1-toddlego-81c25.cloudfunctions.net/testFunction
```

---

## Manual Function Invocation

### Using `invoke-function.js` Script

We've created a helper script to invoke functions easily. See `functions/invoke-function.js`.

**Usage:**
```bash
cd functions

# Invoke a specific function
node invoke-function.js dailyLibraryScraper

# Invoke with options
node invoke-function.js rssFeedParser --region us-central1
```

### Using gcloud CLI (Alternative)

```bash
# List all functions
gcloud functions list --region us-central1

# Call a function
gcloud functions call dailyLibraryScraper \
  --region us-central1 \
  --gen2
```

---

## Verifying Results

### Check Firestore Data

#### Via Firebase Console
1. Go to **Firestore Database**
2. Navigate to `activities` collection
3. Check for new documents
4. Verify fields: `title`, `venue`, `location`, `timing`, etc.

#### Via CLI
```bash
# Install firebase-admin CLI tool (optional)
npm install -g firebase-tools

# Use Firestore emulator for local testing
firebase emulators:start --only firestore
```

#### Via Node.js Script
Create `functions/check-results.js`:
```javascript
const admin = require('firebase-admin');
admin.initializeApp();

async function checkResults() {
  const db = admin.firestore();
  
  // Count activities
  const activitiesSnapshot = await db.collection('activities').get();
  console.log(`Total activities: ${activitiesSnapshot.size}`);
  
  // Get recent activities (last hour)
  const oneHourAgo = Math.floor(Date.now() / 1000) - 3600;
  const recentSnapshot = await db.collection('activities')
    .where('startTime', '>=', oneHourAgo)
    .orderBy('startTime', 'desc')
    .limit(10)
    .get();
  
  console.log(`\nRecent activities (last hour): ${recentSnapshot.size}`);
  recentSnapshot.forEach(doc => {
    const data = doc.data();
    console.log(`- ${data.title} at ${data.venue} (${new Date(data.startTime * 1000).toLocaleString()})`);
  });
}

checkResults().catch(console.error);
```

Run:
```bash
node functions/check-results.js
```

### Verify Function Execution

Check function logs to see execution status:
```bash
# View all function logs
firebase functions:log

# View logs for specific function
firebase functions:log --only dailyLibraryScraper

# Follow logs in real-time
firebase functions:log --tail

# Filter by log level
firebase functions:log --level debug
```

---

## Monitoring & Debugging

### View Real-Time Logs

```bash
# Follow logs in real-time
firebase functions:log --tail

# Filter by function name
firebase functions:log --only dailyLibraryScraper --tail

# Filter by time
firebase functions:log --since 1h
```

### Check Function Status

```bash
# List all functions
firebase functions:list

# Get function details
gcloud functions describe dailyLibraryScraper \
  --region us-central1 \
  --gen2
```

### Debug Common Issues

#### 1. "Function timeout"
- **Cause**: Function taking too long (> 300s for some functions)
- **Solution**: Increase `timeoutSeconds` in function config, or optimize code

#### 2. "API key not found"
- **Cause**: Secret not set or not accessible
- **Solution**: 
  ```bash
  firebase functions:secrets:set SECRET_NAME
  # Redeploy function
  firebase deploy --only functions:functionName
  ```

#### 3. "Permission denied"
- **Cause**: Insufficient IAM permissions
- **Solution**: Grant necessary roles:
  ```bash
  gcloud projects add-iam-policy-binding toddlego-81c25 \
    --member=serviceAccount:YOUR_SERVICE_ACCOUNT \
    --role=roles/cloudfunctions.invoker
  ```

#### 4. "No activities created"
- **Cause**: API failures, data filtering, or geocoding issues
- **Solution**: Check logs for:
  - API errors (rate limits, invalid keys)
  - Date filtering (events in past)
  - Geocoding failures (invalid addresses)

---

## Testing Individual Functions

### 1. dailyLibraryScraper

**Purpose**: Scrapes library websites for toddler events

**Test Steps**:
```bash
# 1. Verify URL registry has entries
# Check Firestore: url_registry collection

# 2. Invoke function
firebase functions:call dailyLibraryScraper

# 3. Check logs
firebase functions:log --only dailyLibraryScraper --tail

# 4. Verify results
# Check Firestore: activities collection
# Look for events with source from libraries
```

**Expected Results**:
- Events with `venue` containing "Library"
- Events with valid `startTime` and `endTime`
- Events geocoded (latitude/longitude present)

### 2. discoverCaliforniaLibraries

**Purpose**: Discovers library websites using Google Places API

**Test Steps**:
```bash
# 1. Verify config_cities has entries
# Check Firestore: config_cities collection

# 2. Invoke function
firebase functions:call discoverCaliforniaLibraries

# 3. Check logs for discovered URLs
firebase functions:log --only discoverCaliforniaLibraries --tail

# 4. Verify results
# Check Firestore: url_registry collection
# Should see new library URLs added
```

**Expected Results**:
- New entries in `url_registry` collection
- URLs pointing to library event pages
- Venue names and locations populated

### 3. serperDevFetchAndFilterEvents

**Purpose**: Searches web for toddler events by county

**Test Steps**:
```bash
# 1. Verify SERPER_DEV_API_KEY is set
firebase functions:secrets:access SERPER_DEV_API_KEY

# 2. Invoke function
firebase functions:call serperDevFetchAndFilterEvents

# 3. Monitor logs (this may take a while)
firebase functions:log --only serperDevFetchAndFilterEvents --tail

# 4. Verify results
# Check Firestore: activities collection
# Look for events from web search sources
```

**Expected Results**:
- Events with various sources (community centers, parks, etc.)
- Events filtered for toddler age range (0-4 years)
- Events with valid dates (future dates only)

### 4. rssFeedParser

**Purpose**: Parses RSS feeds for toddler events

**Test Steps**:
```bash
# 1. Set up RSS feeds in Firestore (optional)
# Collection: rss_feeds
# Document: { url: "https://example.com/events.rss", active: true }

# 2. Invoke function
firebase functions:call rssFeedParser

# 3. Check logs
firebase functions:log --only rssFeedParser --tail

# 4. Verify results
# Check Firestore: activities collection
# Look for events from RSS sources
```

**Expected Results**:
- Events parsed from RSS feeds
- Events with valid dates and descriptions
- Events geocoded to locations

### 5. cityCalendarScraper

**Purpose**: Scrapes city calendar websites for events

**Test Steps**:
```bash
# 1. Set up city calendars in Firestore (optional)
# Collection: city_calendars
# Document: { url: "https://city.gov/events", city: "Oakland, CA", active: true }

# 2. Invoke function
firebase functions:call cityCalendarScraper

# 3. Check logs (may take time due to Gemini processing)
firebase functions:log --only cityCalendarScraper --tail

# 4. Verify results
# Check Firestore: activities collection
# Look for events from city calendars
```

**Expected Results**:
- Events extracted from city websites
- Events filtered for toddler activities
- Events with valid locations and dates

---

## Best Practices

### 1. Test Locally First
Always test functions locally using the emulator before deploying to production.

### 2. Use Test Data
Create test collections/documents in Firestore with sample data to avoid affecting production data.

### 3. Monitor Costs
- Check API usage after each test
- Monitor Firebase quota usage
- Set up billing alerts

### 4. Clean Up Test Data
After testing, clean up any test data created:
```bash
# Use delete_activities.py script
cd scripts
python delete_activities.py --test-only
```

### 5. Check Rate Limits
Be aware of API rate limits:
- **Serper.dev**: 250 queries/month (free tier)
- **Google Maps**: Varies by API type
- **Gemini AI**: Varies by model

### 6. Gradual Rollout
Test functions one at a time, starting with the least critical ones.

---

## Troubleshooting Checklist

- [ ] Firebase CLI is installed and logged in
- [ ] Function dependencies are installed (`npm install`)
- [ ] Firebase secrets are set correctly
- [ ] Firestore rules allow read/write for testing
- [ ] API keys are valid and have quota remaining
- [ ] Function is deployed (`firebase deploy --only functions`)
- [ ] Function has correct IAM permissions
- [ ] Logs show no errors
- [ ] Firestore collections exist and have data
- [ ] Test data is cleaned up after testing

---

## Next Steps

After successful testing:

1. **Monitor Production**: Set up alerting for function failures
2. **Optimize Performance**: Review execution times and optimize slow functions
3. **Scale Gradually**: Monitor activity counts and scale as needed
4. **Document Issues**: Keep a log of any issues encountered and solutions

---

**Last Updated**: January 2025  
**Version**: 1.0.0
