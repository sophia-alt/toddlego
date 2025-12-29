#!/usr/bin/env python3
"""
Seed Bay Area Cities Script
Imports a comprehensive list of Bay Area cities into config_cities collection.
Uses the complete CORE_BAY_AREA_CITIES list for thorough test coverage.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os

# Comprehensive list of Bay Area cities across all 9 counties
CORE_BAY_AREA_CITIES = [
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
]


def initialize_firestore():
    """Initialize Firebase connection"""
    cred = credentials.Certificate("service-account-key.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()


def seed_bayarea_cities():
    """
    Seeds config_cities with all Bay Area cities for comprehensive testing.
    """
    db = initialize_firestore()

    print(f"📍 Seeding {len(CORE_BAY_AREA_CITIES)} Bay Area cities to config_cities...")

    cities_ref = db.collection("config_cities")
    batch = db.batch()
    batch_count = 0

    for city_name in CORE_BAY_AREA_CITIES:
        doc_id = city_name.replace(", ca", "").replace(" ", "_").lower()
        doc_ref = cities_ref.document(doc_id)

        batch.set(
            doc_ref,
            {
                "name": city_name.title(),
                "status": "pending",
                "created_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

        batch_count += 1

        # Firestore batch write limit is 500, commit and start new batch
        if batch_count >= 400:
            batch.commit()
            print(f"  ✅ Committed {batch_count} cities")
            batch = db.batch()
            batch_count = 0

    # Commit remaining
    if batch_count > 0:
        batch.commit()
        print(f"  ✅ Committed {batch_count} cities")

    print(f"\n✨ Successfully seeded {len(CORE_BAY_AREA_CITIES)} Bay Area cities!")
    print(f"\nCities by county:")
    print(f"  • Alameda: 13 cities")
    print(f"  • Contra Costa: 10 cities")
    print(f"  • Marin: 9 cities")
    print(f"  • San Mateo: 18 cities")
    print(f"  • Santa Clara: 13 cities")
    print(f"  • San Francisco: 1 city")
    print(f"  • Solano: 4 cities")
    print(f"  • Napa: 2 cities")
    print(f"\nTotal: {len(CORE_BAY_AREA_CITIES)} Bay Area cities")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    seed_bayarea_cities()
