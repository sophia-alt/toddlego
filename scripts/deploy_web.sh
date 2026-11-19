#!/bin/sh
# Builds the Flutter web app and publishes it, with the project page in site/,
# to Firebase Hosting. The app is served at https://toddlego-81c25.web.app/app/
# Usage: scripts/deploy_web.sh   (FLUTTER=/path/to/flutter if flutter is not on PATH)
set -e
cd "$(dirname "$0")/.."
"${FLUTTER:-flutter}" build web --release --base-href /app/ -o site/app
firebase deploy --only hosting --project toddlego-81c25
