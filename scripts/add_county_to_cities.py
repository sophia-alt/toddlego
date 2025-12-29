#!/usr/bin/env python3
"""
Add county field to all cities in config_cities collection.
Enables county-based deduplication and querying.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os

# Mapping of cities to Bay Area counties
CITY_TO_COUNTY = {
    # Alameda County
    "alameda": "Alameda County",
    "albany": "Alameda County",
    "berkeley": "Alameda County",
    "emeryville": "Alameda County",
    "fremont": "Alameda County",
    "hayward": "Alameda County",
    "livermore": "Alameda County",
    "newark": "Alameda County",
    "oakland": "Alameda County",
    "piedmont": "Alameda County",
    "pleasanton": "Alameda County",
    "san leandro": "Alameda County",
    "union city": "Alameda County",
    # Contra Costa County
    "antioch": "Contra Costa County",
    "brentwood": "Contra Costa County",
    "concord": "Contra Costa County",
    "danville": "Contra Costa County",
    "martinez": "Contra Costa County",
    "moraga": "Contra Costa County",
    "pinole": "Contra Costa County",
    "pittsburg": "Contra Costa County",
    "san ramon": "Contra Costa County",
    "walnut creek": "Contra Costa County",
    # Marin County
    "belvedere": "Marin County",
    "corte madera": "Marin County",
    "larkspur": "Marin County",
    "mill valley": "Marin County",
    "novato": "Marin County",
    "sausalito": "Marin County",
    "san anselmo": "Marin County",
    "san rafael": "Marin County",
    "tiburon": "Marin County",
    # San Mateo County
    "belmont": "San Mateo County",
    "brisbane": "San Mateo County",
    "burlingame": "San Mateo County",
    "daly city": "San Mateo County",
    "east palo alto": "San Mateo County",
    "foster city": "San Mateo County",
    "half moon bay": "San Mateo County",
    "hillsborough": "San Mateo County",
    "menlo park": "San Mateo County",
    "millbrae": "San Mateo County",
    "pacifica": "San Mateo County",
    "palo alto": "San Mateo County",
    "redwood city": "San Mateo County",
    "san bruno": "San Mateo County",
    "san carlos": "San Mateo County",
    "san mateo": "San Mateo County",
    "south san francisco": "San Mateo County",
    "woodside": "San Mateo County",
    # Santa Clara County
    "campbell": "Santa Clara County",
    "cupertino": "Santa Clara County",
    "gilroy": "Santa Clara County",
    "los altos": "Santa Clara County",
    "los altos hills": "Santa Clara County",
    "los gatos": "Santa Clara County",
    "milpitas": "Santa Clara County",
    "morgan hill": "Santa Clara County",
    "mountain view": "Santa Clara County",
    "san jose": "Santa Clara County",
    "santa clara": "Santa Clara County",
    "saratoga": "Santa Clara County",
    "sunnyvale": "Santa Clara County",
    # San Francisco County
    "san francisco": "San Francisco County",
    # Solano County
    "benicia": "Solano County",
    "fairfield": "Solano County",
    "suisun city": "Solano County",
    "vallejo": "Solano County",
    # Napa County
    "napa": "Napa County",
    "yountville": "Napa County",
}

def initialize_firestore():
    """Initialize Firebase connection"""
    cred = credentials.Certificate("service-account-key.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def add_county_to_cities():
    """
    Add county field to all cities in config_cities collection.
    """
    db = initialize_firestore()
    
    print("📍 Fetching all cities from config_cities...")
    cities_ref = db.collection("config_cities")
    cities_snap = cities_ref.stream()
    
    updated = 0
    skipped = 0
    
    for city_doc in cities_snap:
        city_data = city_doc.to_dict() if city_doc.to_dict() else {}
        city_name = city_data.get("name", "").lower().strip()
        
        # Extract base city name (remove ", ca" if present)
        base_name = city_name.replace(", ca", "").strip()
        
        # Look up county
        county = CITY_TO_COUNTY.get(base_name)
        
        if county:
            city_doc.reference.update({"county": county})
            print(f"  ✅ Updated {city_name.title()} → {county}")
            updated += 1
        else:
            print(f"  ⚠️  Skipped {city_name.title()} (no county mapping)")
            skipped += 1
    
    print(f"\n✨ Done! Updated {updated} cities with county field.")
    if skipped > 0:
        print(f"⚠️  Skipped {skipped} cities without county mapping.")

if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    add_county_to_cities()
