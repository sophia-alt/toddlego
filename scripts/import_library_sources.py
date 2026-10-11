#!/usr/bin/env python3
"""
Import public library systems and branches from the IMLS Public Libraries Survey (PLS)
into Firestore, replacing the Google Places-based discovery job.

The PLS outlet file lists every US public library branch with its address and
geocoded coordinates (LATITUDE / LONGITUD). It has no website column, so the event
calendar platform of each system comes from library_calendar_platforms.json, which
is maintained by hand.

Writes two collections:
  library_systems/{FSCSKEY}               e.g. CA0001 (Alameda County Library)
  library_branches/{FSCSKEY}-{FSCS_SEQ}   e.g. CA0001-008 (Dublin Library)

Usage:
  python import_library_sources.py --dry-run          # parse and print a summary only
  python import_library_sources.py                    # California (default)
  python import_library_sources.py --state CA --state NV
  python import_library_sources.py --all-states
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import zipfile

import certifi
import requests

# FY2023 PLS public-use data files (released 2025). Pass --zip-url for a newer year.
DEFAULT_PLS_ZIP_URL = "https://www.imls.gov/sites/default/files/2025-08/pls_fy2023_csv.zip"
PLS_DATA_YEAR = "FY2023"

PLATFORMS_FILE = os.path.join(os.path.dirname(__file__), "library_calendar_platforms.json")

# C_OUT_TY: CE = central library, BR = branch, BS = bookmobile, BM = books-by-mail.
# Bookmobiles and books-by-mail have no fixed place to hold storytime.
STATIONARY_OUTLET_TYPES = {"CE", "BR"}

FIRESTORE_BATCH_SIZE = 400

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat, lng, precision=10):
    """Standard geohash, same output as geofire-common's geohashForLocation (precision 10)."""
    lat_range = [-90.0, 90.0]
    lng_range = [-180.0, 180.0]
    chars = []
    bit = 0
    ch = 0
    even = True
    while len(chars) < precision:
        rng, val = (lng_range, lng) if even else (lat_range, lat)
        mid = (rng[0] + rng[1]) / 2
        if val >= mid:
            ch = (ch << 1) | 1
            rng[0] = mid
        else:
            ch = ch << 1
            rng[1] = mid
        even = not even
        bit += 1
        if bit == 5:
            chars.append(_GEOHASH_BASE32[ch])
            bit = 0
            ch = 0
    return "".join(chars)


def download_pls_zip(url):
    """Download the PLS zip with retries (certifi avoids macOS SSL issues)."""
    for attempt in range(1, 4):
        try:
            resp = requests.get(
                url,
                timeout=120,
                verify=certifi.where(),
                headers={"User-Agent": "Mozilla/5.0 (ToddleGo library source import)"},
            )
            resp.raise_for_status()
            return zipfile.ZipFile(io.BytesIO(resp.content))
        except Exception as e:
            print(f"⚠️ Download attempt {attempt}/3 failed: {e}")
            time.sleep(2 * attempt)
    print("❌ Could not download the PLS data. Download it manually and pass --zip-file.")
    sys.exit(1)


def read_csv_from_zip(zf, name_fragment):
    """Read the CSV whose file name contains name_fragment (case-insensitive)."""
    matches = [
        n for n in zf.namelist()
        if n.lower().endswith(".csv") and name_fragment in os.path.basename(n).lower()
    ]
    if len(matches) != 1:
        print(f"❌ Expected one '{name_fragment}' CSV in the zip, found: {matches}")
        sys.exit(1)
    # PLS files are Latin-1 encoded (accented names such as "PEÑASCO").
    text = zf.read(matches[0]).decode("latin-1")
    return list(csv.DictReader(io.StringIO(text)))


def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_sources(outlets, systems, platforms, states):
    """Turn PLS rows into (systems_by_key, branches_by_id) for the requested states."""
    system_docs = {}
    for row in systems:
        if states and row["STABR"] not in states:
            continue
        # C_FSCS = N marks entities outside the public library definition, mostly
        # county law libraries; they hold no children's programs.
        if row["C_FSCS"] != "Y":
            continue
        key = row["FSCSKEY"]
        platform = platforms.get(key) or {}
        system_docs[key] = {
            "name": row["LIBNAME"].strip(),
            "state": row["STABR"],
            "city": row["CITY"].strip(),
            "county": row["CNTY"].strip(),
            "calendar_platform": platform.get("platform"),
            "calendar_tenant": platform.get("tenant"),
            "calendar_events_url": platform.get("events_url"),
            "branch_count": 0,
            "pls_year": PLS_DATA_YEAR,
        }

    branch_docs = {}
    skipped = {"outlet_type": 0, "no_system": 0, "no_coordinates": 0}
    for row in outlets:
        if states and row["STABR"] not in states:
            continue
        if row["C_OUT_TY"] not in STATIONARY_OUTLET_TYPES:
            skipped["outlet_type"] += 1
            continue
        system_key = row["FSCSKEY"]
        if system_key not in system_docs:
            skipped["no_system"] += 1
            continue
        lat = parse_float(row["LATITUDE"])
        lng = parse_float(row["LONGITUD"])
        if lat is None or lng is None or (lat == 0 and lng == 0):
            skipped["no_coordinates"] += 1
            continue

        branch_id = f"{system_key}-{row['FSCS_SEQ']}"
        branch_codes = (platforms.get(system_key) or {}).get("branch_codes", {})
        branch_docs[branch_id] = {
            "name": row["LIBNAME"].strip(),
            "system_id": system_key,
            "system_name": system_docs[system_key]["name"],
            "state": row["STABR"],
            "address": row["ADDRESS"].strip(),
            "city": row["CITY"].strip(),
            "zip": row["ZIP"].strip(),
            "county": row["CNTY"].strip(),
            "phone": row["PHONE"].strip() or None,
            "outlet_type": row["C_OUT_TY"],
            "latitude": lat,
            "longitude": lng,
            "geohash": encode_geohash(lat, lng),
            # POINTADDRESS / STREETADDRESS are rooftop or street-level; POSTAL and
            # LOCALITY are zip or town centroids, too coarse for "15 minutes away".
            "geo_match_type": row["GEOMTYPE"],
            "calendar_location_code": branch_codes.get(branch_id),
            "pls_year": PLS_DATA_YEAR,
        }
        system_docs[system_key]["branch_count"] += 1

    return system_docs, branch_docs, skipped


def write_collection(db, collection, docs):
    from firebase_admin import firestore

    items = list(docs.items())
    for start in range(0, len(items), FIRESTORE_BATCH_SIZE):
        batch = db.batch()
        for doc_id, data in items[start:start + FIRESTORE_BATCH_SIZE]:
            batch.set(
                db.collection(collection).document(doc_id),
                {**data, "imported_at": firestore.SERVER_TIMESTAMP},
                merge=True,
            )
        batch.commit()
        print(f"   ✅ {collection}: committed {min(start + FIRESTORE_BATCH_SIZE, len(items))}/{len(items)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state", action="append", help="Two-letter state code; repeatable (default: CA)")
    parser.add_argument("--all-states", action="store_true", help="Import every state")
    parser.add_argument("--zip-url", default=DEFAULT_PLS_ZIP_URL, help="PLS CSV zip to download")
    parser.add_argument("--zip-file", help="Use a local PLS CSV zip instead of downloading")
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without writing to Firestore")
    args = parser.parse_args()

    states = None if args.all_states else {s.upper() for s in (args.state or ["CA"])}

    with open(PLATFORMS_FILE, encoding="utf-8") as f:
        platforms = {k: v for k, v in json.load(f).items() if not k.startswith("_")}

    if args.zip_file:
        zf = zipfile.ZipFile(args.zip_file)
    else:
        print(f"⏳ Downloading IMLS PLS data ({PLS_DATA_YEAR})...")
        zf = download_pls_zip(args.zip_url)

    outlets = read_csv_from_zip(zf, "outlet")
    systems = read_csv_from_zip(zf, "_ae_")
    system_docs, branch_docs, skipped = build_sources(outlets, systems, platforms, states)

    with_platform = [s for s in system_docs.values() if s["calendar_platform"]]
    covered = sum(s["branch_count"] for s in with_platform)
    print(f"📚 States: {', '.join(sorted(states)) if states else 'all'}")
    print(f"   {len(system_docs)} library systems, {len(branch_docs)} branches")
    print(f"   Skipped outlets: {skipped}")
    print(f"   {len(with_platform)} systems ({covered} branches) have a known calendar platform")

    missing = [k for k in platforms if k not in system_docs and (not states or k[:2] in states)]
    if missing:
        print(f"⚠️ library_calendar_platforms.json keys not found in PLS data: {missing}")

    if args.dry_run:
        print("ℹ️ Dry run: nothing written.")
        return

    import firebase_admin
    from firebase_admin import credentials, firestore

    cred = credentials.Certificate(os.path.join(os.path.dirname(__file__), "service-account-key.json"))
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    print("📦 Writing to Firestore...")
    write_collection(db, "library_systems", system_docs)
    write_collection(db, "library_branches", branch_docs)
    print("🎉 Done")


if __name__ == "__main__":
    main()
