#!/bin/bash
# Script to manually delete old Cloud Scheduler jobs that Firebase is trying to clean up
# Run this before deploying to avoid 404 errors

PROJECT_ID="toddlego-81c25"
REGION="us-central1"

echo "Attempting to delete old Cloud Scheduler jobs..."

# Try to delete old jobs (ignore errors if they don't exist)
gcloud scheduler jobs delete firebase-schedule-serpApiFetchAndFilterEvents-${REGION} \
    --location=${REGION} \
    --project=${PROJECT_ID} \
    2>/dev/null || echo "Job serpApiFetchAndFilterEvents not found (already deleted or never existed)"

gcloud scheduler jobs delete firebase-schedule-fetchAndFilterEvents-${REGION} \
    --location=${REGION} \
    --project=${PROJECT_ID} \
    2>/dev/null || echo "Job fetchAndFilterEvents not found (already deleted or never existed)"

echo "Cleanup complete. You can now deploy without 404 errors."
