#!/usr/bin/env node

/**
 * Helper script to check function execution results in Firestore
 * Usage: node check-results.js [options]
 */

const admin = require('firebase-admin');
const path = require('path');
const fs = require('fs');

// Initialize Firebase Admin with projectId for local runs (e.g. gcloud auth application-default login)
const projectId = process.env.GCLOUD_PROJECT || process.env.GCLOUD_PROJECT_ID || 'toddlego-81c25';
const keyPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || path.join(__dirname, '../scripts/service-account-key.json');

let initOptions = { projectId };
if (fs.existsSync(keyPath)) {
  try {
    const key = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
    initOptions.credential = admin.credential.cert(key);
  } catch (e) {
    // Ignore; will use default credentials (e.g. gcloud auth application-default login)
  }
}

try {
  admin.initializeApp(initOptions);
} catch (e) {
  if (!e.message || !e.message.includes('already exists')) {
    try {
      admin.initializeApp(initOptions);
    } catch (e2) {
      // Already initialized
    }
  }
}

const db = admin.firestore();

async function checkResults() {
  console.log('📊 Checking Firestore results...\n');

  try {
    // Count total activities
    const activitiesSnapshot = await db.collection('activities').get();
    console.log(`📦 Total activities in database: ${activitiesSnapshot.size}`);

    // Get recent activities (last 24 hours)
    const oneDayAgo = Math.floor(Date.now() / 1000) - 86400;
    const recentSnapshot = await db.collection('activities')
      .where('startTime', '>=', oneDayAgo)
      .orderBy('startTime', 'desc')
      .limit(20)
      .get();

    console.log(`\n🆕 Recent activities (last 24 hours): ${recentSnapshot.size}`);

    if (recentSnapshot.size > 0) {
      console.log('\n📋 Recent activities:');
      console.log('─'.repeat(80));
      let index = 0;
      recentSnapshot.forEach((doc) => {
        index += 1;
        const data = doc.data();
        const startDate = new Date(data.startTime * 1000).toLocaleString();
        const venue = data.venue || 'Unknown venue';
        const title = data.title || 'Untitled event';
        const source = data.sourceUrl ? new URL(data.sourceUrl).hostname : 'Unknown';
        
        console.log(`${index}. ${title}`);
        console.log(`   Venue: ${venue}`);
        console.log(`   Date: ${startDate}`);
        console.log(`   Source: ${source}`);
        if (data.latitude && data.longitude) {
          console.log(`   Location: ${data.latitude.toFixed(4)}, ${data.longitude.toFixed(4)}`);
        }
        console.log('');
      });
    } else {
      console.log('   No recent activities found.');
    }

    // Check URL registry
    const urlRegistrySnapshot = await db.collection('url_registry').get();
    console.log(`\n🔗 URL registry entries: ${urlRegistrySnapshot.size}`);

    // Check geocoding cache
    const geocodingCacheSnapshot = await db.collection('geocoding_cache').get();
    console.log(`\n🗺️  Geocoding cache entries: ${geocodingCacheSnapshot.size}`);

    // Check config cities
    const configCitiesSnapshot = await db.collection('config_cities').get();
    console.log(`\n🏙️  Configured cities: ${configCitiesSnapshot.size}`);

    // Future activities count (next 30 days)
    const thirtyDaysFromNow = Math.floor(Date.now() / 1000) + (30 * 86400);
    const futureSnapshot = await db.collection('activities')
      .where('startTime', '<=', thirtyDaysFromNow)
      .where('startTime', '>=', Math.floor(Date.now() / 1000))
      .get();
    
    console.log(`\n📅 Upcoming activities (next 30 days): ${futureSnapshot.size}`);

    // Activities by source (sample)
    const sources = {};
    activitiesSnapshot.forEach(doc => {
      const data = doc.data();
      if (data.sourceUrl) {
        try {
          const hostname = new URL(data.sourceUrl).hostname;
          sources[hostname] = (sources[hostname] || 0) + 1;
        } catch (e) {
          sources['unknown'] = (sources['unknown'] || 0) + 1;
        }
      } else {
        sources['no-source'] = (sources['no-source'] || 0) + 1;
      }
    });

    if (Object.keys(sources).length > 0) {
      console.log(`\n📊 Activities by source (top 10):`);
      const sortedSources = Object.entries(sources)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10);
      sortedSources.forEach(([source, count]) => {
        console.log(`   ${source}: ${count}`);
      });
    }

    console.log('\n✅ Check complete!\n');

  } catch (error) {
    console.error('❌ Error checking results:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run check
checkResults()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  });
