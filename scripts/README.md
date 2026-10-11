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

Import library systems and branches with `import_library_sources.py` (below).

---

# Library Source Table (IMLS Public Libraries Survey)

`import_library_sources.py` replaces the old `discoverCaliforniaLibraries` Cloud Function, which
searched Google Places for each city (~$15 per run) and mostly found library home pages.

It downloads the free [IMLS Public Libraries Survey](https://www.imls.gov/research-evaluation/data-collection/public-libraries-survey)
data (FY2023: ~9,000 library systems and ~17,000 outlets nationwide, each geocoded) and writes:

| Collection | Doc ID | Contents |
|---|---|---|
| `library_systems` | IMLS `FSCSKEY`, e.g. `CA0001` | Name, state, county, calendar platform and tenant, branch count |
| `library_branches` | `FSCSKEY-FSCS_SEQ`, e.g. `CA0001-008` | Name, address, latitude/longitude, geohash, geocode match type, system, platform location code |

Bookmobiles, books-by-mail and non-public entities (county law libraries) are skipped.
In California that leaves 186 systems and 1,127 branches.

The PLS data has no website column, so which calendar platform a system uses
(BiblioCommons, LibraryMarket, LibCal, ...) lives in `library_calendar_platforms.json`, keyed by
`FSCSKEY`. **Expanding to a new region is mostly adding entries to that file** and re-running
the import. A system's key is the `FSCSKEY` column next to its `LIBNAME` in the IMLS
administrative entity file (`PLS_FY23_AE_*.csv`), or the doc ID in `library_systems`.

### Usage

```bash
python import_library_sources.py --dry-run                 # parse and summarize, no writes
python import_library_sources.py                           # California (default)
python import_library_sources.py --state CA --state NV     # several states
python import_library_sources.py --all-states
python import_library_sources.py --zip-file pls_fy2023_csv.zip   # use a local download
```

Re-running is safe: documents are merged by ID. When IMLS publishes a newer year, pass
`--zip-url` (and update `DEFAULT_PLS_ZIP_URL` / `PLS_DATA_YEAR` in the script).

The scrapers do not read these collections yet; platform adapters (architecture review
2026-10, section 2) will.

## Notes

- **One-time operation:** Run this once. Re-running will overwrite existing documents.
- **Data source:** California Open Data - Incorporated Cities dataset
- **Safety:** Uses batching to stay under Firestore's 500-write limit
