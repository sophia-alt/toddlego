# Quick Start: Testing Guide

## 🚀 Quick Testing Steps

### 1. **Deploy Functions First** (if not already deployed)
```bash
cd apps/toddlego
firebase deploy --only functions
```

### 2. **Test via Firebase Console** (Easiest Method)

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select project: `toddlego-81c25`
3. Navigate to **Functions** → **Cloud Functions**
4. Click on a function (e.g., `dailyLibraryScraper`)
5. Click **"Test"** tab
6. Click **"Test the function"** button
7. Wait for execution (~30-300 seconds depending on function)
8. Check logs and execution status

### 3. **Test via Command Line**

```bash
cd apps/toddlego

# Test a specific function
firebase functions:call dailyLibraryScraper

# Or use the helper script
cd functions
node invoke-function.js dailyLibraryScraper
```

### 4. **Check Results**

```bash
cd apps/toddlego/functions

# Check Firestore results
node check-results.js
```

Or check directly in Firebase Console:
- Go to **Firestore Database**
- Check `activities` collection for new entries
- Verify fields: `title`, `venue`, `location`, `timing`, etc.

### 5. **View Logs**

```bash
# View all logs
firebase functions:log

# View logs for specific function
firebase functions:log --only dailyLibraryScraper --tail

# Follow logs in real-time
firebase functions:log --tail
```

---

## 📋 Functions to Test

### Available Functions:

1. **dailyLibraryScraper** (Daily library events)
   ```bash
   firebase functions:call dailyLibraryScraper
   ```

2. **discoverCaliforniaLibraries** (Monthly library discovery)
   ```bash
   firebase functions:call discoverCaliforniaLibraries
   ```

3. **serperDevFetchAndFilterEvents** (Weekly web search)
   ```bash
   firebase functions:call serperDevFetchAndFilterEvents
   ```

4. **rssFeedParser** (Weekly RSS feed parsing)
   ```bash
   firebase functions:call rssFeedParser
   ```

6. **cityCalendarScraper** (Weekly city calendar scraping)
   ```bash
   firebase functions:call cityCalendarScraper
   ```

---

## ✅ Verification Checklist

After running a function, verify:

- [ ] Function executed successfully (check logs)
- [ ] No errors in execution logs
- [ ] Activities collection has new entries (check Firestore)
- [ ] New activities have valid data:
  - [ ] Title and venue
  - [ ] Valid dates (future dates only)
  - [ ] Location (latitude/longitude)
  - [ ] Age range (toddler-appropriate: 0-4 years)
- [ ] Activity count increased in `check-results.js` output

---

## 🐛 Common Issues

### Issue: "Function not found"
**Solution**: Deploy functions first
```bash
firebase deploy --only functions
```

### Issue: "Secret not found" 
**Solution**: Set Firebase secrets
```bash
firebase functions:secrets:set GEMINI_API_KEY
firebase functions:secrets:set GOOGLE_MAPS_API_KEY
firebase functions:secrets:set SERPER_DEV_API_KEY
```

### Issue: "No activities created"
**Possible causes**:
- API failures (check logs)
- Events filtered out (past dates, wrong age range)
- Geocoding failures (invalid addresses)
- No data sources available (check `url_registry`, `rss_feeds`, `city_calendars`)

### Issue: "Function timeout"
**Solution**: Function may be processing large amounts of data. Check logs for progress. Some functions may take 5+ minutes.

---

## 📚 For More Details

See comprehensive guide: `functions/TESTING_GUIDE.md`

---

**Last Updated**: January 2025
