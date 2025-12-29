#!/usr/bin/env python3
"""
Delete all activities from Firestore.
Useful for clean testing before running SerpApi ingestion.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os

def initialize_firestore():
    """Initialize Firebase connection"""
    cred = credentials.Certificate("service-account-key.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def delete_collection(collection_path, batch_size=100):
    """
    Recursively delete all documents in a collection.
    """
    db = initialize_firestore()
    docs = db.collection(collection_path).limit(batch_size).stream()
    
    deleted = 0
    for doc in docs:
        print(f"  🗑️  Deleting: {doc.id}")
        doc.reference.delete()
        deleted += 1
    
    if deleted >= batch_size:
        return deleted + delete_collection(collection_path, batch_size)
    
    return deleted

if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    
    print("⚠️  This will DELETE ALL documents in the 'activities' collection!")
    response = input("Continue? (yes/no): ")
    
    if response.lower() != "yes" or response.lower() != "y":
        print("❌ Cancelled. No documents deleted.")
        exit(0)
    
    print("\n🗑️  Deleting all activities...")
    total_deleted = delete_collection("activities")
    print(f"\n✨ Done! Deleted {total_deleted} documents from activities collection.")
