#!/usr/bin/env node
/**
 * Seed config_cities with Bay Area cities (alternative to Python scripts/seed_bayarea_cities.py).
 * Run from repo root with: GOOGLE_APPLICATION_CREDENTIALS=scripts/service-account-key.json node functions/seed-config-cities.js
 * Or from functions/: GOOGLE_APPLICATION_CREDENTIALS=../scripts/service-account-key.json node seed-config-cities.js
 */

const admin = require("firebase-admin");
const path = require("path");
const fs = require("fs");

const BAY_AREA_CITIES = [
    "alameda, ca", "albany, ca", "berkeley, ca", "emeryville, ca", "fremont, ca", "hayward, ca",
    "livermore, ca", "newark, ca", "oakland, ca", "piedmont, ca", "pleasanton, ca", "san leandro, ca", "union city, ca",
    "antioch, ca", "brentwood, ca", "concord, ca", "danville, ca", "martinez, ca", "moraga, ca",
    "pinole, ca", "pittsburg, ca", "san ramon, ca", "walnut creek, ca",
    "belvedere, ca", "corte madera, ca", "larkspur, ca", "mill valley, ca", "novato, ca",
    "sausalito, ca", "san anselmo, ca", "san rafael, ca", "tiburon, ca",
    "belmont, ca", "brisbane, ca", "burlingame, ca", "daly city, ca", "east palo alto, ca", "foster city, ca",
    "half moon bay, ca", "hillsborough, ca", "menlo park, ca", "millbrae, ca", "pacifica, ca", "palo alto, ca",
    "redwood city, ca", "san bruno, ca", "san carlos, ca", "san mateo, ca", "south san francisco, ca", "woodside, ca",
    "campbell, ca", "cupertino, ca", "gilroy, ca", "los altos, ca", "los altos hills, ca", "los gatos, ca",
    "milpitas, ca", "morgan hill, ca", "mountain view, ca", "san jose, ca", "santa clara, ca", "saratoga, ca", "sunnyvale, ca",
    "san francisco, ca",
    "benicia, ca", "fairfield, ca", "suisun city, ca", "vallejo, ca",
    "napa, ca", "yountville, ca",
];

const CITY_TO_COUNTY = {
    alameda: "Alameda County", albany: "Alameda County", berkeley: "Alameda County", emeryville: "Alameda County",
    fremont: "Alameda County", hayward: "Alameda County", livermore: "Alameda County", newark: "Alameda County",
    oakland: "Alameda County", piedmont: "Alameda County", pleasanton: "Alameda County", "san leandro": "Alameda County", "union city": "Alameda County",
    antioch: "Contra Costa County", brentwood: "Contra Costa County", concord: "Contra Costa County", danville: "Contra Costa County",
    martinez: "Contra Costa County", moraga: "Contra Costa County", pinole: "Contra Costa County", pittsburg: "Contra Costa County",
    "san ramon": "Contra Costa County", "walnut creek": "Contra Costa County",
    belvedere: "Marin County", "corte madera": "Marin County", larkspur: "Marin County", "mill valley": "Marin County",
    novato: "Marin County", sausalito: "Marin County", "san anselmo": "Marin County", "san rafael": "Marin County", tiburon: "Marin County",
    belmont: "San Mateo County", brisbane: "San Mateo County", burlingame: "San Mateo County", "daly city": "San Mateo County",
    "east palo alto": "San Mateo County", "foster city": "San Mateo County", "half moon bay": "San Mateo County", hillsborough: "San Mateo County",
    "menlo park": "San Mateo County", millbrae: "San Mateo County", pacifica: "San Mateo County", "palo alto": "San Mateo County",
    "redwood city": "San Mateo County", "san bruno": "San Mateo County", "san carlos": "San Mateo County", "san mateo": "San Mateo County",
    "south san francisco": "San Mateo County", woodside: "San Mateo County",
    campbell: "Santa Clara County", cupertino: "Santa Clara County", gilroy: "Santa Clara County", "los altos": "Santa Clara County",
    "los altos hills": "Santa Clara County", "los gatos": "Santa Clara County", milpitas: "Santa Clara County", "morgan hill": "Santa Clara County",
    "mountain view": "Santa Clara County", "san jose": "Santa Clara County", "santa clara": "Santa Clara County", saratoga: "Santa Clara County", sunnyvale: "Santa Clara County",
    "san francisco": "San Francisco County",
    benicia: "Solano County", fairfield: "Solano County", "suisun city": "Solano County", vallejo: "Solano County",
    napa: "Napa County", yountville: "Napa County",
};

async function main() {
    const projectId = process.env.GCLOUD_PROJECT || process.env.GCLOUD_PROJECT_ID || "toddlego-81c25";
    const keyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || path.join(__dirname, "../scripts/service-account-key.json");

    const initOptions = { projectId };
    if (fs.existsSync(keyPath)) {
        try {
            initOptions.credential = admin.credential.cert(JSON.parse(fs.readFileSync(keyPath, "utf8")));
        } catch (e) {
            console.warn("Could not load service account key:", e.message);
        }
    }

    try {
        admin.initializeApp(initOptions);
    } catch (e) {
        if (!e.message || !e.message.includes("already exists")) throw e;
    }

    const db = admin.firestore();
    const col = db.collection("config_cities");
    let committed = 0;

    console.log("Seeding config_cities with", BAY_AREA_CITIES.length, "Bay Area cities...");

    for (let i = 0; i < BAY_AREA_CITIES.length; i += 400) {
        const batch = db.batch();
        const chunk = BAY_AREA_CITIES.slice(i, i + 400);
        for (const city of chunk) {
            const docId = city.replace(/, ca$/i, "").replace(/\s+/g, "_").trim().toLowerCase();
            const county = CITY_TO_COUNTY[docId.replace(/_/g, " ")] || null;
            batch.set(col.doc(docId), {
                name: city.replace(/, ca$/i, "").replace(/\b\w/g, (c) => c.toUpperCase()) + ", CA",
                status: "pending",
                county: county ? `${county}, CA` : null,
                created_at: admin.firestore.FieldValue.serverTimestamp(),
            }, { merge: true });
        }
        await batch.commit();
        committed += chunk.length;
        console.log("  Committed", committed, "cities");
    }

    console.log("Done. config_cities seeded with", BAY_AREA_CITIES.length, "cities.");
    process.exit(0);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
