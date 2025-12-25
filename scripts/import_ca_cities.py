#!/usr/bin/env python3
"""
Import California Cities to Firestore
Fetches official CA city data and uploads to 'config_cities' collection
Run once locally to seed the database for the discovery function
"""

import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import sys
import io
import os
import time
import requests
import certifi


def import_ca_cities():
    """Download CA cities from official data source and upload to Firestore"""

    # 1. Initialize Firestore
    # Replace with the path to your downloaded JSON key
    cred = credentials.Certificate("./service-account-key.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    # Official CA Open Data CSV for Incorporated Cities
    csv_url = (
        "https://data.ca.gov/dataset/e21184f6-6ef0-4f33-96cb-75179462a48a/"
        "resource/03433a04-5178-4394-a1f9-93666f91605e/download/california-incorporated-cities.csv"
    )

    print("⏳ Downloading city data from California Open Data...")

    # Prefer requests with certifi to avoid macOS SSL issues
    df = None
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(csv_url, timeout=30, verify=certifi.where())
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            break
        except Exception as e:
            print(f"⚠️ Download attempt {attempt}/{max_retries} failed: {e}")
            time.sleep(2 * attempt)

    # Fallback: use local file if present
    if df is None:
        local_path = os.path.join(os.path.dirname(__file__), "california-incorporated-cities.csv")
        if os.path.exists(local_path):
            print(f"📄 Using local CSV fallback: {local_path}")
            try:
                df = pd.read_csv(local_path)
            except Exception as e:
                print(f"❌ Failed to read local CSV: {e}")
                sys.exit(1)
        else:
            print("❌ Error downloading CSV and no local fallback found.")
            print("Tip: You can manually download the file and place it next to this script as 'california-incorporated-cities.csv'.")
            # Minimal bootstrap: seed with a curated list of major CA cities
            fallback_cities = [
                "Los Angeles",
                "San Diego",
                "San Jose",
                "San Francisco",
                "Fresno",
                "Sacramento",
                "Long Beach",
                "Oakland",
                "Bakersfield",
                "Anaheim",
                "Riverside",
                "Stockton",
                "Irvine",
                "Chula Vista",
                "Fremont",
                "San Bernardino",
                "Modesto",
                "Oxnard",
                "Fontana",
                "Moreno Valley",
                "Glendale",
                "Huntington Beach",
                "Santa Clarita",
                "Garden Grove",
                "Santa Rosa",
                "Oceanside",
                "Rancho Cucamonga",
                "Ontario",
                "Elk Grove",
                "Corona",
                "Lancaster",
                "Palmdale",
                "Hayward",
                "Salinas",
                "Pomona",
                "Sunnyvale",
                "Escondido",
                "Torrance",
                "Pasadena",
                "Orange",
                "Fullerton",
            ]
            df = pd.DataFrame({"CITY": fallback_cities})

    # Extract unique city names (adjust column name if dataset varies)
    if "CITY" not in df.columns:
        print("❌ Expected 'CITY' column not found. Available columns:", df.columns.tolist())
        sys.exit(1)

    cities = df["CITY"].unique().tolist()
    print(f"✅ Found {len(cities)} California cities")

    # 2. Batch Upload (Firestore limit is 500 per batch)
    print("📦 Starting batch upload to Firestore...")
    batch = db.batch()
    count = 0

    for city_name in cities:
        # Create a document ID that is URL-friendly
        doc_id = city_name.lower().replace(" ", "_").replace("-", "_")
        doc_ref = db.collection("config_cities").document(doc_id)

        # Initialize with 'pending' status for Discovery Function
        batch.set(
            doc_ref,
            {
                "name": f"{city_name}, CA",
                "status": "pending",
                "last_scanned": None,
            },
        )

        count += 1

        # Commit every 400 to stay safely under the 500 limit
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"   ✅ Committed {count} cities...")

    # Commit remaining
    if count % 400 != 0:
        batch.commit()

    print(f"\n🎉 Success! Imported {count} California cities to 'config_cities' collection")
    print("The Discovery Function can now iterate through these cities")


if __name__ == "__main__":
    import_ca_cities()
