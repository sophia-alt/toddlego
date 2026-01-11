#!/usr/bin/env node

/**
 * Helper script to manually invoke Cloud Functions for testing
 * Usage: node invoke-function.js <functionName> [options]
 */

const { execSync } = require('child_process');
const path = require('path');

// Parse command line arguments
const args = process.argv.slice(2);
const functionName = args[0];

if (!functionName) {
    console.error('❌ Error: Function name is required');
    console.log('\nUsage: node invoke-function.js <functionName> [options]');
    console.log('\nAvailable functions:');
    console.log('  - dailyLibraryScraper');
    console.log('  - discoverCaliforniaLibraries');
    console.log('  - serperDevFetchAndFilterEvents');
    console.log('  - rssFeedParser');
    console.log('  - cityCalendarScraper');
    console.log('\nExample:');
    console.log('  node invoke-function.js dailyLibraryScraper');
    process.exit(1);
}

const availableFunctions = [
    'dailyLibraryScraper',
    'discoverCaliforniaLibraries',
    'serperDevFetchAndFilterEvents',
    'rssFeedParser',
    'cityCalendarScraper',
];

if (!availableFunctions.includes(functionName)) {
    console.error(`❌ Error: Unknown function "${functionName}"`);
    console.log(`\nAvailable functions: ${availableFunctions.join(', ')}`);
    process.exit(1);
}

// Get project root directory (parent of functions/)
const projectRoot = path.resolve(__dirname, '..');

console.log(`🚀 Invoking function: ${functionName}`);
console.log(`📁 Project root: ${projectRoot}\n`);

try {
    // Change to project root and invoke function
    const command = `firebase functions:call ${functionName}`;
    console.log(`Running: ${command}\n`);

    execSync(command, {
        cwd: projectRoot,
        stdio: 'inherit',
    });

    console.log(`\n✅ Function "${functionName}" invocation completed`);
    console.log(`\n💡 Tip: View logs with:`);
    console.log(`   firebase functions:log --only ${functionName} --tail`);

} catch (error) {
    console.error(`\n❌ Error invoking function: ${error.message}`);
    console.log(`\n💡 Make sure you are:`);
    console.log('   1. Logged in: firebase login');
    console.log('   2. In the correct project: firebase use <project-id>');
    console.log('   3. Function is deployed: firebase deploy --only functions');
    process.exit(1);
}
