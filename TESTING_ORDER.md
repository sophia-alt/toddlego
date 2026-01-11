# Testing Order Guide

This guide provides a recommended order for testing your Cloud Functions after deployment.

---

## ✅ Deployment Status

All functions have been deployed successfully:
- ✔️ `dailyLibraryScraper` (Daily)
- ✔️ `discoverCaliforniaLibraries` (Monthly)
- ✔️ `serperDevFetchAndFilterEvents` (Weekly)
- ✔️ `rssFeedParser` (Daily)
- ✔️ `cityCalendarScraper` (Weekly)

---

## 🧪 Recommended Testing Order

### 1. **Start with Web Search** (Easiest - No setup required)

**Function:** `serperDevFetchAndFilterEvents`

**Why first:** 
- Uses free API (Serper.dev)
- No data sources to configure
- Quick results (5-10 minutes)

**Test:**
```bash
cd apps/toddlego
firebase functions:call serperDevFetchAndFilterEvents

# Watch logs
firebase functions:log --only serperDevFetchAndFilterEvents --tail
```

**Expected:** Events found from web search, saved to Firestore

**Verify:**
```bash
cd functions
node check-results.js
```

---

### 2. **Test RSS Feed Parser** (Quick - Simple setup)

**Function:** `rssFeedParser`

**Why second:**
- Structured data (RSS feeds)
- Fast processing
- Easy to verify results

**Setup (optional):**
- Add RSS feed URLs to Firestore collection `rss_feeds`
- Or function will skip if no feeds configured

**Test:**
```bash
firebase functions:call rssFeedParser

# Watch logs
firebase functions:log --only rssFeedParser --tail
```

**Expected:** Events parsed from RSS feeds (if feeds configured)

**Verify:**
```bash
cd functions
node check-results.js
```

---

### 3. **Test Library Discovery** (Medium complexity)

**Function:** `discoverCaliforniaLibraries`

**Why third:**
- Populates `url_registry` for daily scraper
- One-time setup (runs monthly)
- Takes longer (10-20 minutes)

**Prerequisites:**
- Ensure `config_cities` collection has entries
- Requires Google Maps API key (for Places API)

**Test:**
```bash
firebase functions:call discoverCaliforniaLibraries

# Watch logs (may take 10-20 minutes)
firebase functions:log --only discoverCaliforniaLibraries --tail
```

**Expected:** New library URLs added to `url_registry` collection

**Verify in Firebase Console:**
- Go to Firestore Database
- Check `url_registry` collection
- Should see new library URLs

---

### 4. **Test Daily Library Scraper** (Requires step 3)

**Function:** `dailyLibraryScraper`

**Why fourth:**
- Requires `url_registry` entries (from step 3)
- Uses Gemini AI (takes time)
- Daily function (most important for regular updates)

**Prerequisites:**
- `url_registry` collection should have entries
- Requires GEMINI_API_KEY and GOOGLE_MAPS_API_KEY

**Test:**
```bash
firebase functions:call dailyLibraryScraper

# Watch logs (may take 5-15 minutes)
firebase functions:log --only dailyLibraryScraper --tail
```

**Expected:** Events extracted from library websites, saved to Firestore

**Verify:**
```bash
cd functions
node check-results.js
```

---

### 5. **Test City Calendar Scraper** (Optional - Requires setup)

**Function:** `cityCalendarScraper`

**Why last:**
- Requires manual setup (city calendar URLs)
- Uses Gemini AI (slower)
- Optional data source

**Setup required:**
- Add city calendar URLs to Firestore collection `city_calendars`
- Structure: `{ url, city_name, venue_name, latitude, longitude }`

**Test:**
```bash
firebase functions:call cityCalendarScraper

# Watch logs (may take 5-10 minutes per calendar)
firebase functions:log --only cityCalendarScraper --tail
```

**Expected:** Events extracted from city calendars (if configured)

**Verify:**
```bash
cd functions
node check-results.js
```

---

## 📋 Quick Test Checklist

Use this checklist to track your testing progress:

- [ ] **1. Test serperDevFetchAndFilterEvents** (Web search)
  - [ ] Function executed successfully
  - [ ] Logs show events found
  - [ ] Events appear in Firestore
  - [ ] Verified with `check-results.js`

- [ ] **2. Test rssFeedParser** (RSS feeds)
  - [ ] Function executed successfully
  - [ ] Logs show feeds processed (if configured)
  - [ ] Events appear in Firestore (if feeds configured)

- [ ] **3. Test discoverCaliforniaLibraries** (Library discovery)
  - [ ] Function executed successfully
  - [ ] Logs show libraries discovered
  - [ ] New URLs in `url_registry` collection

- [ ] **4. Test dailyLibraryScraper** (Library events)
  - [ ] Function executed successfully
  - [ ] Logs show events extracted
  - [ ] Events appear in Firestore
  - [ ] Verified with `check-results.js`

- [ ] **5. Test cityCalendarScraper** (City calendars - optional)
  - [ ] Function executed successfully (if configured)
  - [ ] Logs show events extracted (if configured)
  - [ ] Events appear in Firestore (if configured)

---

## 🚀 Quick Test Commands

**Test all functions in order:**
```bash
cd apps/toddlego

# 1. Web search (quickest)
firebase functions:call serperDevFetchAndFilterEvents

# 2. RSS feeds
firebase functions:call rssFeedParser

# 3. Library discovery (takes longer)
firebase functions:call discoverCaliforniaLibraries

# 4. Library scraper (requires step 3)
firebase functions:call dailyLibraryScraper

# 5. City calendars (optional)
firebase functions:call cityCalendarScraper
```

**Check results after each test:**
```bash
cd functions
node check-results.js
```

**Watch logs for any function:**
```bash
firebase functions:log --only FUNCTION_NAME --tail
```

**View all logs:**
```bash
firebase functions:log --tail
```

---

## 🔍 Verification Steps

### Check Firestore Data

1. **Via Firebase Console:**
   - Go to https://console.firebase.google.com/project/toddlego-81c25/firestore
   - Check `activities` collection
   - Verify new events have valid data:
     - ✅ Title and venue
     - ✅ Valid dates (future dates)
     - ✅ Location (latitude/longitude)
     - ✅ Age range (0-4 years)

2. **Via Script:**
   ```bash
   cd functions
   node check-results.js
   ```

### Check Function Logs

```bash
# View logs for specific function
firebase functions:log --only FUNCTION_NAME --tail

# View all logs
firebase functions:log --tail

# View recent logs
firebase functions:log --since 1h
```

---

## ⚠️ Common Issues

### Issue: "No events created"

**Possible causes:**
- Function executed but no matching events found
- Events filtered out (wrong age range, past dates)
- API failures (check logs)
- Missing data sources (RSS feeds, city calendars not configured)

**Solutions:**
- Check function logs for errors
- Verify data sources are configured
- Check if events match filters (toddler age range, future dates)

### Issue: "Function timeout"

**Possible causes:**
- Processing large amounts of data
- Slow API responses
- Network issues

**Solutions:**
- Normal for some functions (library scraper can take 10-15 minutes)
- Check logs for progress
- Functions have 300-second timeout (5 minutes)

### Issue: "API key not found"

**Possible causes:**
- Secret not set in Firebase

**Solutions:**
```bash
firebase functions:secrets:set SECRET_NAME
# Redeploy function
firebase deploy --only functions:FUNCTION_NAME
```

---

## 📊 Expected Results

After testing all functions, you should see:

- **Activities Collection:** Multiple events from different sources
- **Event Sources:** Mix of web search, RSS feeds, libraries, city calendars
- **Event Quality:** Valid titles, venues, dates, locations
- **Age Range:** Mostly 0-4 years (toddler-appropriate)
- **Location Coverage:** Bay Area locations (San Francisco, Oakland, etc.)

---

## 🎯 Next Steps After Testing

1. **Monitor Production:** Check that scheduled functions run automatically
2. **Add Data Sources:** Configure RSS feeds and city calendars for better coverage
3. **Optimize:** Review execution times and optimize slow functions
4. **Monitor Costs:** Check API usage and costs in Firebase Console

---

**Last Updated:** January 2025  
**Status:** Ready for Testing
