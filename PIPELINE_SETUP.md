# Pipeline setup: get more activities in the app

Follow these steps **in order** so the app has enough activities to show.

## 1. Seed cities (required for library discovery)

**Option A – Node (no Python):**
```bash
cd apps/toddlego
export GOOGLE_APPLICATION_CREDENTIALS=scripts/service-account-key.json
node functions/seed-config-cities.js
```

**Option B – Python:**
```bash
cd apps/toddlego/scripts
pip install -r requirements.txt
python seed_bayarea_cities.py
python add_county_to_cities.py
```

You need a Firebase **service account key** in `scripts/service-account-key.json` (download from Firebase Console → Project settings → Service accounts → Generate new private key).

## 2. Run discovery (fills `url_registry` for the daily scraper)

- **Firebase Console:** Functions → `discoverCaliforniaLibraries` → Run / Test.
- **CLI:** `firebase functions:call discoverCaliforniaLibraries`

Discovery runs **weekly** (Sunday 00:00 UTC). After the first run, check Firestore: **url_registry** should have documents.

## 3. Run the daily library scraper (fills `activities`)

- **Firebase Console:** Functions → `dailyLibraryScraper` → Run / Test.
- **CLI:** `firebase functions:call dailyLibraryScraper`

This runs **daily** on schedule; you can trigger it once manually. Check logs for "Events Added".

## 4. Optional: add more sources

- **RSS:** The app now includes Oakland Public Library’s EventKeeper feed. Add more in Firestore collection **rss_feeds** (fields: `url`, `venue_name`, `city`, optional `latitude`/`longitude`).
- **City calendars:** SF Rec & Park and Oakland events are hardcoded. Add more in Firestore **city_calendars** (`url`, `city_name`, `venue_name`).

## 5. Check results

```bash
cd apps/toddlego/functions
export GOOGLE_APPLICATION_CREDENTIALS=../scripts/service-account-key.json
node check-results.js
```

Or use **Firebase Console → Firestore** and check:

- **config_cities** – many docs
- **url_registry** – many docs after discovery
- **activities** – docs with `startTime` in the future (app shows only today and future)

## Changes made to fix “too few activities”

- **Discovery** runs **weekly** instead of monthly so `url_registry` fills sooner.
- **RSS:** Oakland Public Library EventKeeper feed is hardcoded so the RSS job adds events without Firestore config.
- **City calendars:** SF Rec & Park and Oakland events URLs are hardcoded so the city calendar scraper runs without Firestore config.
- **check-results.js** can use `GOOGLE_APPLICATION_CREDENTIALS` and `GCLOUD_PROJECT` for local runs.
- **seed-config-cities.js** (Node) seeds Bay Area cities so you can run the pipeline without Python.
