# California Cities Importer

This script imports official California city data from the California Open Data portal and seeds your Firestore `config_cities` collection.

## Setup

1. **Download your Firebase service account key:**
   - Go to [Firebase Console](https://console.firebase.google.com/project/toddlego-81c25/settings/serviceaccounts/adminsdk)
   - Click "Generate New Private Key"
   - Save as `service-account-key.json` in this directory

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the script once to import all California cities:

```bash
python import_ca_cities.py
```

**Expected output:**
```
⏳ Downloading city data from California Open Data...
✅ Found 482 California cities
📦 Starting batch upload to Firestore...
   ✅ Committed 400 cities...
🎉 Success! Imported 482 California cities to 'config_cities' collection
```

## What it does

- ✅ Downloads official CA city list from [data.ca.gov](https://data.ca.gov)
- ✅ Parses city names
- ✅ Uploads to Firestore `config_cities` collection with `status: "pending"`
- ✅ Creates URL-friendly document IDs (e.g., "san_francisco")

## Next Step

The `discoverCaliforniaLibraries` Cloud Function can now iterate through these cities and search for libraries using Google Places API.

## Notes

- **One-time operation:** Run this once. Re-running will overwrite existing documents.
- **Data source:** California Open Data - Incorporated Cities dataset
- **Safety:** Uses batching to stay under Firestore's 500-write limit
