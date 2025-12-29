#!/usr/bin/env python3
"""
Filter Bay Area Cities Script
Removes all cities from config_cities that are outside the Bay Area.
Keeps only the core Bay Area counties for focused testing.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os
import sys

# Bay Area geographic bounds (approximate)
# Latitude: 36.5 - 38.5, Longitude: -123.5 to -121.0
BAY_AREA_BOUNDS = {
    "lat_min": 36.5,
    "lat_max": 38.5,
    "lng_min": -123.5,
    "lng_max": -121.0,
}

# Core Bay Area cities (for explicit filtering)
CORE_BAY_AREA_CITIES = {
    # Alameda County
    "alameda, ca",
    "albany, ca",
    "berkeley, ca",
    "emeryville, ca",
    "fremont, ca",
    "hayward, ca",
    "livermore, ca",
    "newark, ca",
    "oakland, ca",
    "piedmont, ca",
    "pleasanton, ca",
    "san leandro, ca",
    "union city, ca",
    # Contra Costa County
    "antioch, ca",
    "brentwood, ca",
    "concord, ca",
    "danville, ca",
    "martinez, ca",
    "moraga, ca",
    "pinole, ca",
    "pittsburg, ca",
    "san ramon, ca",
    "walnut creek, ca",
    # Marin County
    "belvedere, ca",
    "corte madera, ca",
    "larkspur, ca",
    "mill valley, ca",
    "novato, ca",
    "sausalito, ca",
    "san anselmo, ca",
    "san rafael, ca",
    "tiburon, ca",
    # San Mateo County
    "belmont, ca",
    "brisbane, ca",
    "burlingame, ca",
    "daly city, ca",
    "east palo alto, ca",
    "foster city, ca",
    "half moon bay, ca",
    "hillsborough, ca",
    "menlo park, ca",
    "millbrae, ca",
    "pacifica, ca",
    "palo alto, ca",
    "redwood city, ca",
    "san bruno, ca",
    "san carlos, ca",
    "san mateo, ca",
    "south san francisco, ca",
    "woodside, ca",
    # Santa Clara County
    "campbell, ca",
    "cupertino, ca",
    "gilroy, ca",
    "los altos, ca",
    "los altos hills, ca",
    "los gatos, ca",
    "milpitas, ca",
    "morgan hill, ca",
    "mountain view, ca",
    "san jose, ca",
    "santa clara, ca",
    "saratoga, ca",
    "sunnyvale, ca",
    # San Francisco County
    "san francisco, ca",
    # Solano County
    "benicia, ca",
    "fairfield, ca",
    "suisun city, ca",
    "vallejo, ca",
    # Napa County (edge of Bay Area)
    "napa, ca",
    "yountville, ca",
}


def initialize_firestore():
    """Initialize Firebase connection"""
    cred = credentials.Certificate("service-account-key.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()


def filter_bayarea_cities():
    """
    Query all cities from config_cities, identify non-Bay Area cities, and delete them.
    """
    db = initialize_firestore()

    print("📍 Fetching all cities from config_cities...")
    cities_ref = db.collection("config_cities")
    cities_snap = cities_ref.stream()

    cities_to_delete = []
    cities_to_keep = []

    for city_doc in cities_snap:
        city_data = dict(city_doc.to_dict()) if city_doc.to_dict() else {}
        city_name = city_data.get("name", "").lower().strip()

        # Check if city is in Bay Area
        is_bayarea = city_name in CORE_BAY_AREA_CITIES

        if is_bayarea:
            cities_to_keep.append(city_name)
        else:
            cities_to_delete.append((city_doc.id, city_name))

    # Summary
    print(f"\n✅ Bay Area cities to KEEP ({len(cities_to_keep)}):")
    for city in sorted(cities_to_keep):
        print(f"  • {city.title()}")

    print(f"\n❌ Non-Bay Area cities to DELETE ({len(cities_to_delete)}):")
    for doc_id, city_name in cities_to_delete:
        print(f"  • {city_name.title()}")

    # Confirm before deletion
    if cities_to_delete:
        response = input(
            f"\n⚠️  Delete {len(cities_to_delete)} non-Bay Area cities? (yes/no): "
        )
        if response.lower() != "yes":
            print("❌ Cancelled. No cities deleted.")
            return

        # Delete non-Bay Area cities
        print("\n🗑️  Deleting non-Bay Area cities...")
        for doc_id, city_name in cities_to_delete:
            cities_ref.document(doc_id).delete()
            print(f"  ✅ Deleted: {city_name}")

        print(f"\n✨ Done! Kept {len(cities_to_keep)} Bay Area cities.")
    else:
        print("\n✨ All cities are already Bay Area cities!")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    filter_bayarea_cities()
