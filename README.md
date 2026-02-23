# Toddlego - Toddler Activity Finder

A comprehensive Flutter application that helps parents find activities for toddlers (ages 0-4) in California, with a focus on the Bay Area. The app combines real-time location-based filtering with automated event discovery from multiple sources.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Code Structure](#code-structure)
- [Design Decisions](#design-decisions)
- [Data Models](#data-models)
- [API Integrations](#api-integrations)
- [Features](#features)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Development Workflow](#development-workflow)

---

## 🎯 Project Overview

**Toddlego** is a cross-platform mobile application built with Flutter that aggregates and displays toddler-friendly events from multiple sources. The app provides:

- **Real-time event discovery** from libraries, community centers, and online sources
- **Location-based filtering** with customizable radius (5, 10, 25 miles)
- **Distance-based sorting** to show nearest events first
- **Automated data collection** via scheduled cloud functions
- **Smart caching** to minimize API costs and improve performance

### Target Audience
Parents and caregivers looking for activities for children aged 0-4 years in California, with primary focus on the San Francisco Bay Area.

### Tech Stack
- **Frontend**: Flutter (Dart 3.10.4+)
- **Backend**: Firebase (Firestore, Cloud Functions, Hosting)
- **Data Pipeline**: Python scripts for initial data seeding
- **APIs**: Google Maps, Gemini AI, Serper.dev

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Flutter Mobile App                       │
│  (iOS, Android, Web, macOS, Linux, Windows)                  │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  UI Layer    │  │  State Mgmt  │  │  Models      │     │
│  │  (main.dart) │  │  (Stateful)  │  │ (activity.dart)│   │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└───────────────────────┬──────────────────────────────────────┘
                        │
                        │ Real-time Stream
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Firebase Backend                          │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │              Firestore Database                     │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │    │
│  │  │ activities   │  │ url_registry │  │config_    │ │    │
│  │  │              │  │              │  │cities     │ │    │
│  │  │              │  │              │  │           │ │    │
│  │  └──────────────┘  └──────────────┘  └──────────┘ │    │
│  │  ┌──────────────┐                                   │    │
│  │  │geocoding_    │                                   │    │
│  │  │cache         │                                   │    │
│  │  └──────────────┘                                   │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │          Cloud Functions (Node.js)                  │    │
│  │                                                      │    │
│  │  • dailyLibraryScraper (Daily)                      │    │
│  │  • discoverCaliforniaLibraries (Monthly)            │    │
│  │  • serperDevFetchAndFilterEvents (Weekly)           │    │
│  └────────────────────────────────────────────────────┘    │
└───────────────────────┬──────────────────────────────────────┘
                        │
                        │ Scheduled Jobs & Web Scraping
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              External APIs & Data Sources                    │
│                                                              │
│  • Google Maps API (Geocoding, Places)                      │
│  • Google Gemini AI (Event Extraction)                      │
│  • Serper.dev (Web Search)                                  │
│  • Jina Reader (Web Content Extraction)                     │
│  • Library Websites (Direct Scraping)                       │
└─────────────────────────────────────────────────────────────┘
```

### Three-Tier Architecture

1. **Presentation Layer** (Flutter App)
   - UI components and user interaction
   - Location services
   - Real-time data streaming
   - State management

2. **Application Layer** (Firebase Cloud Functions)
   - Event discovery and aggregation
   - Data processing and normalization
   - Cache management
   - API orchestration

3. **Data Layer** (Firestore)
   - Activity storage
   - Configuration data
   - Caching layers
   - URL registry

---

## 📁 Code Structure

### Project Directory Layout

```
apps/toddlego/
├── lib/                          # Flutter application source code
│   ├── main.dart                # Main app entry point, UI components
│   ├── models/
│   │   └── activity.dart        # Activity data model
│   └── firebase_options.dart    # Firebase configuration (generated)
│
├── functions/                    # Firebase Cloud Functions
│   ├── index.js                 # All cloud functions (1,516 lines)
│   ├── package.json             # Node.js dependencies
│   └── node_modules/            # NPM packages
│
├── scripts/                      # Python data pipeline scripts
│   ├── import_ca_cities.py      # Import CA cities from official source
│   ├── seed_bayarea_cities.py   # Seed Bay Area cities for testing
│   ├── add_county_to_cities.py  # Add county information to cities
│   ├── filter_bayarea_cities.py # Filter to Bay Area cities only
│   ├── delete_activities.py     # Utility to delete activities
│   ├── requirements.txt         # Python dependencies
│   └── service-account-key.json # Firebase Admin SDK key
│
├── android/                      # Android platform-specific code
├── ios/                          # iOS platform-specific code
├── web/                          # Web platform-specific code
├── macos/                        # macOS platform-specific code
├── linux/                        # Linux platform-specific code
├── windows/                      # Windows platform-specific code
│
├── test/                         # Unit and widget tests
│   └── widget_test.dart
│
├── pubspec.yaml                  # Flutter dependencies
├── firebase.json                 # Firebase configuration
├── analysis_options.yaml         # Dart/Flutter linting rules
└── README.md                     # This file
```

### Detailed Code Organization

#### Frontend (`lib/`)

**`lib/main.dart`** (551 lines)
- **Entry Point**: `main()` - Initializes Flutter and Firebase
- **App Widget**: `ToddlerActivityApp` - Root MaterialApp configuration
- **Main Screen**: `ActivityListScreen` - Primary UI with location filtering
  - Location permission handling
  - Distance calculation and caching
  - Activity filtering and sorting
  - Radius selector UI
- **UI Components**: `ActivityCard` - Displays individual activity information
  - Badge rendering (FREE, etc.)
  - Info chips (date, time, age range)
  - Distance display
  - Navigation placeholder

**`lib/models/activity.dart`** (120 lines)
- **Data Model**: `Activity` class
  - Properties: id, title, venue, description, timing, location, etc.
  - Factory constructor: `Activity.fromFirestore()` - Parses Firestore documents
  - Supports both flat and nested data structures (backward compatibility)
  - Validation and error handling

#### Backend (`functions/index.js`) (1,516 lines)

**Helper Functions**:
- `getDynamicCoordinates()` - Geocoding with caching (6-month TTL)
- `generateEventId()` - Unique event ID generation
- `normalizeAgeRange()` - Age range standardization
- `cleanContentForHashing()` - Content normalization for caching
- `generateContentHash()` - SHA-256 hashing for cache keys

**Active Cloud Functions**:

1. **`dailyLibraryScraper`** (Lines 198-527)
   - **Schedule**: Every 24 hours
   - **Purpose**: Scrapes library websites for toddler events
   - **Process**:
     - Reads from `url_registry` collection
     - Fetches website content via Jina Reader
     - Uses Gemini AI to extract events
     - Caches content hashes to avoid reprocessing
     - Batch uploads to `activities` collection

2. **`discoverCaliforniaLibraries`** (Lines 531-692)
   - **Schedule**: Monthly (1st of month 00:00 UTC)
   - **Purpose**: Discovers library websites using Google Places API
   - **Process**:
     - Iterates through `config_cities` collection
     - Searches for libraries and parks
     - Stores URLs in `url_registry` for daily scraper

3. **`serperDevFetchAndFilterEvents`** (Lines 694-1110)
   - **Schedule**: Every Sunday at midnight UTC
   - **Purpose**: Searches web for toddler events by county
   - **Process**:
     - Uses 2 targeted queries per county (optimized from 5)
     - Extracts events using Gemini AI
     - Geocodes locations with fallback
     - Filters and validates events
     - Deduplicates before saving


#### Data Pipeline (`scripts/`)

**Python Scripts**:
- **`import_ca_cities.py`**: Imports all CA cities from official data source
- **`seed_bayarea_cities.py`**: Seeds 70+ Bay Area cities for testing
- **`add_county_to_cities.py`**: Adds county information to city documents
- **`filter_bayarea_cities.py`**: Filters to Bay Area cities only
- **`delete_activities.py`**: Utility to clean up activities collection

---

## 🎨 Design Decisions

### 1. **Real-Time Data Streaming**

**Decision**: Use Firestore `snapshots()` for real-time updates

**Rationale**:
- Users see new events immediately without refreshing
- Automatic UI updates when data changes
- Efficient - only sends changes, not full datasets

**Implementation**:
```dart
Stream<QuerySnapshot> _activitiesTodayAndFuture() {
  final todayStartSec = DateTime.now().millisecondsSinceEpoch ~/ 1000;
  return FirebaseFirestore.instance
      .collection('activities')
      .where('startTime', isGreaterThanOrEqualTo: todayStartSec)
      .orderBy('startTime', descending: false)
      .snapshots();
}
```

### 2. **Client-Side Distance Filtering**

**Decision**: Calculate distances in Flutter app rather than Firestore queries

**Rationale**:
- Firestore geohash queries require specific field structure
- Client-side filtering is more flexible
- Caching distances avoids recalculation
- User's location changes frequently

**Trade-off**: All future events are downloaded, then filtered. Acceptable for current scale (~1000 events).

### 3. **Dual Schema Support**

**Decision**: Support both flat and nested data structures in Activity model

**Rationale**:
- Backward compatibility during schema migration
- Flexibility for future enhancements
- Handles both old and new event formats

**Example**:
```dart
// Supports both:
{ latitude: 37.7749, longitude: -122.4194 }  // Flat
{ location: { geopoint: GeoPoint(...) } }    // Nested
```

### 4. **Aggressive Caching Strategy**

**Decision**: Cache geocoding results for 6 months, content hashes indefinitely

**Rationale**:
- Venue locations rarely change
- Reduces Google Maps API costs significantly
- Improves response times
- Content hashing prevents reprocessing unchanged websites

**Implementation**:
- Geocoding cache: `geocoding_cache` collection (6-month TTL)
- Content cache: Stored in `url_registry` documents (content_hash)

### 5. **Rate Limiting & Cost Optimization**

**Decision**: Reduce API calls, add delays, disable expensive functions

**Rationale**:
- Prevent hitting API rate limits
- Control costs (Google Maps API charges per request)
- Ensure reliable execution
- Stay under Serper.dev quota (250 queries/month)

**Changes Applied**:
- Serper.dev: Reduced from 5 to 2 queries per county
- Added 1.5-2s delays between API calls
- Limited results processing (30 per county for Gemini)
- Disabled expensive Google Places Events function

### 6. **Error Handling Strategy**

**Decision**: Graceful degradation with detailed error messages

**Rationale**:
- Don't crash entire app if one event has bad data
- Provide user-friendly error messages
- Log errors for debugging
- Allow retry actions

**Implementation**:
- Try-catch around Activity parsing
- Validation in `fromFirestore()` factory
- Error UI with retry button
- Continue processing if individual items fail

### 7. **Modular Function Design**

**Decision**: Separate functions for different data sources

**Rationale**:
- Independent scheduling
- Easy to enable/disable specific sources
- Isolated failures don't affect others
- Easier to monitor and debug

---

## 📊 Data Models

### Activity Model

**Firestore Collection**: `activities`

```dart
class Activity {
  final String id;              // Document ID (generated hash)
  final String title;           // Event title (required)
  final String venue;           // Venue name (required)
  final String? description;    // Optional description
  final int startTime;          // Unix timestamp (seconds)
  final int endTime;            // Unix timestamp (seconds)
  final String ageRange;        // "0-2", "2-4", "All", etc.
  final bool isIndoor;          // Indoor vs outdoor
  final String sourceUrl;       // Original source URL
  final bool isFree;            // Free event flag
  final bool requiresBooking;   // Registration required
  final String? registrationUrl;// Registration link
  final double latitude;        // Location latitude
  final double longitude;       // Location longitude
  final String? type;           // "one_time", "recurring", "static"
  final bool? isAllDay;         // All-day event flag
}
```

**Firestore Document Structure** (Dual Schema):

```javascript
// Flat Structure (Legacy)
{
  title: "Storytime",
  venue: "Oakland Public Library",
  latitude: 37.8044,
  longitude: -122.2712,
  startTime: 1704067200,  // Unix timestamp
  endTime: 1704070800,
  ageRange: "0-4 years",
  isFree: true,
  // ... other fields
}

// Nested Structure (New)
{
  title: "Storytime",
  venue: "Oakland Public Library",
  location: {
    name: "Oakland Public Library",
    geopoint: GeoPoint(37.8044, -122.2712),
    geohash: "9q8yy..."
  },
  timing: {
    start_time: 1704067200,
    end_time: 1704070800,
    is_all_day: false,
    recurrence: null
  },
  age_range: [0, 4],
  // ... other fields
}
```

### Supporting Collections

**`config_cities`** - California cities configuration
```javascript
{
  name: "San Francisco, CA",
  status: "pending" | "scanning" | "complete" | "error",
  county: "San Francisco County, CA",
  last_scanned: Timestamp,
  libraries_found: number,
  // ... other metadata
}
```

**`url_registry`** - Library websites to scrape
```javascript
{
  url_hash: "https://oaklandlibrary.org/events",
  venue_name: "Oakland Public Library",
  city: "Oakland, CA",
  latitude: 37.8044,
  longitude: -122.2712,
  content_hash: "abc123...",  // SHA-256 hash
  last_parsed: 1704067200,
  event_count: 5,
  // ... other metadata
}
```

**`geocoding_cache`** - Geocoding results cache
```javascript
{
  venueName: "Oakland Public Library",
  lat: 37.8044,
  lng: -122.2712,
  address: "125 14th St, Oakland, CA 94612",
  cachedAt: 1704067200000  // Milliseconds timestamp
}
```

---

## 🔌 API Integrations

### Active APIs

1. **Firebase Services**
   - **Firestore**: Real-time database
   - **Cloud Functions**: Serverless backend
   - **Hosting**: Web app deployment

2. **Google Maps API**
   - **Geocoding**: Convert addresses to coordinates
   - **Places API**: Discover venues (used in discovery function)
   - **Caching**: 6-month TTL to minimize costs
   - **Usage**: ~600-900 requests/month (mostly cached)

3. **Google Gemini AI**
   - **Model**: `gemini-2.0-flash`
   - **Purpose**: Extract events from unstructured content
   - **Usage**: Only when content changes (cached)
   - **Estimated**: ~300 calls/month

4. **Serper.dev API**
   - **Purpose**: Web search for toddler events
   - **Quota**: 250 queries/month (free tier)
   - **Usage**: 64 queries/month (2 queries × 8 counties × 4 weeks)
   - **Status**: Well under limit (26% usage)

5. **Jina Reader API**
   - **Purpose**: Extract clean markdown from library websites
   - **Usage**: ~50 requests/day (daily scraper)
   - **Caching**: Content hashing prevents reprocessing


---

## ✨ Features

### Core Features

1. **Real-Time Event Discovery**
   - Automatic event updates via Firestore streams
   - Multiple data sources (libraries, web search, APIs)
   - Scheduled data collection

2. **Location-Based Filtering**
   - GPS-based location detection
   - Customizable search radius (5, 10, 25 miles)
   - Distance-based sorting (nearest first)
   - Works without location (shows all events)

3. **Activity Information**
   - Event title and description
   - Venue name and location
   - Date and time
   - Age range
   - Free/paid indication
   - Registration requirements
   - Distance from user

4. **Smart Caching**
   - Geocoding cache (6-month TTL)
   - Content hash caching (prevents reprocessing)
   - Distance calculation caching

5. **Error Handling**
   - Graceful degradation
   - User-friendly error messages
   - Retry mechanisms
   - Detailed logging

### UI Features

- **Material Design 3** theme (orange color scheme)
- **Responsive cards** with activity information
- **Badges** for FREE events
- **Info chips** for date, time, age range
- **Distance highlighting** (< 2 miles in bold)
- **Location error dialog** with retry option
- **Empty state messages** with helpful text
- **Loading indicators** during data fetch

---

## ⚙️ Configuration

### Firebase Secrets

Required secrets (set via `firebase functions:secrets:set`):
- `GEMINI_API_KEY` - Google Gemini AI API key
- `GOOGLE_MAPS_API_KEY` - Google Maps API key
- `SERPER_DEV_API_KEY` - Serper.dev API key

### Environment Variables

**Flutter App** (`firebase_options.dart`):
- Auto-generated by `flutterfire configure`
- Contains Firebase project configuration
- Platform-specific implementations

**Cloud Functions** (`functions/index.js`):
- Secrets accessed via `defineSecret()` and `.value()`
- Fallback to `process.env` for local development

### Flutter Dependencies (`pubspec.yaml`)

**Core**:
- `flutter`: SDK
- `firebase_core: ^4.3.0`
- `firebase_auth: ^6.1.3`
- `cloud_firestore: ^6.1.1`
- `geolocator: ^14.0.2`

**Dev**:
- `flutter_test`: SDK
- `flutter_lints: ^6.0.0`

### Node.js Dependencies (`functions/package.json`)

**Core**:
- `firebase-admin: ^13.6.0`
- `firebase-functions: ^7.0.0`
- `@google/generative-ai: ^0.24.1`
- `@googlemaps/google-maps-services-js: ^3.4.2`
- `axios: ^1.13.2`
- `geofire-common: ^6.0.0`

**Python Dependencies** (`scripts/requirements.txt`):
- `firebase-admin: 6.4.0`
- `pandas: 2.2.0`
- `requests: >=2.31.0`
- `certifi: >=2023.7.22`

---

## 🚀 Deployment

### Prerequisites

1. **Firebase Project Setup**:
   ```bash
   # Install Firebase CLI
   npm install -g firebase-tools
   
   # Login to Firebase
   firebase login
   
   # Initialize project (if not done)
   firebase init
   ```

2. **Flutter Setup**:
   ```bash
   # Install Flutter SDK
   # Configure FlutterFire
   flutter pub global activate flutterfire_cli
   flutterfire configure
   ```

### Deploy Flutter Web App

```bash
# Build for web
flutter build web

# Deploy to Firebase Hosting
firebase deploy --only hosting
```

### Deploy Cloud Functions

```bash
cd apps/toddlego

# Install dependencies
cd functions
npm install
cd ..

# Deploy all functions
firebase deploy --only functions

# Or deploy specific function
firebase deploy --only functions:dailyLibraryScraper
```

### Set Firebase Secrets

```bash
# Set each secret (you'll be prompted to enter the value)
firebase functions:secrets:set GEMINI_API_KEY
firebase functions:secrets:set GOOGLE_MAPS_API_KEY
firebase functions:secrets:set SERPER_DEV_API_KEY

# View all secrets
firebase functions:secrets:access
```

### Initial Data Setup

**See [PIPELINE_SETUP.md](PIPELINE_SETUP.md)** for a step-by-step guide to get more activities showing in the app.

1. **Seed Cities** (one-time):
   ```bash
   cd scripts
   python import_ca_cities.py
   # OR for Bay Area only:
   python seed_bayarea_cities.py
   # OR use Node (no Python): node functions/seed-config-cities.js
   ```

2. **Run Discovery** (or wait for scheduled run):
   ```bash
   # Trigger manually via Firebase Console
   # Or wait for monthly scheduled run (1st of month 00:00 UTC)
   ```

3. **Verify Setup**:
   - Check `config_cities` collection in Firestore
   - Check `url_registry` collection after discovery
   - Check `activities` collection after scrapers run

---

## 💻 Development Workflow

### Local Development

1. **Flutter App**:
   ```bash
   # Install dependencies
   flutter pub get
   
   # Run on device/emulator
   flutter run
   
   # Run on web
   flutter run -d chrome
   
   # Run tests
   flutter test
   ```

2. **Cloud Functions** (Emulator):
   ```bash
   cd functions
   
   # Install dependencies
   npm install
   
   # Start emulator
   firebase emulators:start --only functions
   
   # In another terminal, test function
   firebase functions:shell
   > dailyLibraryScraper()
   ```

3. **Python Scripts**:
   ```bash
   cd scripts
   
   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Run script
   python import_ca_cities.py
   ```

### Testing

**Flutter Tests**:
- Unit tests: `test/widget_test.dart`
- Widget tests: Test individual components
- Integration tests: Full app flow

**Cloud Functions**:
- **📖 See `functions/TESTING_GUIDE.md` for comprehensive testing guide**
- Use Firebase Functions emulator: `firebase emulators:start --only functions`
- Test manually: `node functions/invoke-function.js <functionName>`
- Check results: `node functions/check-results.js`
- Monitor logs: `firebase functions:log`
- Test via Firebase Console: Functions → Test tab

**Python Scripts**:
- Run with test data first
- Verify Firestore writes
- Check batch sizes (stay under 500 limit)

### Code Quality

**Linting**:
```bash
# Flutter/Dart
flutter analyze

# Fix issues
dart fix --apply
```

**Formatting**:
```bash
# Flutter/Dart
dart format lib/

# Node.js (functions)
cd functions
npx prettier --write index.js
```

---

## 📈 Performance Considerations

### Optimizations Applied

1. **Distance Calculation Caching**
   - Calculate once per activity
   - Store in map for reuse
   - Avoid recalculation during sorting

2. **Firestore Query Optimization**
   - Filter by `startTime >= today` on server
   - Client-side filtering for distance only
   - Index on `startTime` for efficient queries

3. **Rate Limiting**
   - Delays between API calls
   - Batch processing
   - Caching to minimize API usage

4. **Content Caching**
   - SHA-256 hashing for content changes
   - Skip Gemini processing if content unchanged
   - 6-month geocoding cache

### Scalability

**Current Scale**:
- ~100-200 events per day
- ~700-1000 events total in database
- ~50-100 libraries in registry

**Future Scaling**:
- Consider Firestore geohash queries for location filtering
- Implement pagination for large result sets
- Add composite indexes for complex queries
- Consider CDN for static assets

---

## 🔒 Security & Privacy

### Data Privacy

- **No Personal Data**: App doesn't collect user information
- **Location Data**: Stays on device, not stored
- **Public Data Only**: All activities are publicly available events

### API Security

- **Secrets Management**: Use Firebase Secrets (never commit)
- **API Keys**: Stored securely in Firebase Secrets
- **Service Account**: Python scripts use service account JSON (gitignored)

### Firestore Security Rules

**Recommendation**: Set up security rules:

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /activities/{activityId} {
      allow read: if true;  // Public read
      allow write: if false;  // Only via Admin SDK
    }
    
    match /config_cities/{cityId} {
      allow read: if true;
      allow write: if false;
    }
    
    // Other collections...
  }
}
```

---

## 🐛 Known Issues & Limitations

1. **Client-Side Filtering**: All future events downloaded, then filtered
   - **Impact**: Minor for current scale
   - **Solution**: Use Firestore geohash queries for large scale

2. **No Pagination**: Shows all filtered events at once
   - **Impact**: Performance with >100 events
   - **Solution**: Implement pagination or virtual scrolling

3. **Limited Date Parsing**: Some date formats may fail
   - **Impact**: Some events with unusual date formats skipped
   - **Solution**: Use date parsing library (e.g., `date-fns`)

4. **Geocoding Failures**: Events dropped if geocoding fails
   - **Impact**: Some events lost
   - **Mitigation**: Fallback to city center coordinates (implemented)

5. **No Offline Support**: Requires internet connection
   - **Impact**: No functionality without network
   - **Solution**: Implement Firestore offline persistence

---

## 🔮 Future Enhancements

### Planned Features

1. **Activity Details Screen**: Navigate to full event details
2. **Favorites/Bookmarks**: Save events for later
3. **Notifications**: Alert for new nearby events
4. **Calendar Integration**: Add events to device calendar
5. **Map View**: Show events on interactive map
6. **Filters**: Filter by age range, free/paid, indoor/outdoor
7. **Search**: Search events by keyword
8. **Offline Mode**: Cache events for offline viewing

### Technical Improvements

1. **Firestore Geohash Queries**: Server-side location filtering
2. **Pagination**: Handle large result sets efficiently
3. **Date Parsing Library**: Better date format support
4. **Unit Tests**: Comprehensive test coverage
5. **Integration Tests**: End-to-end testing
6. **Performance Monitoring**: Firebase Performance Monitoring
7. **Crash Reporting**: Firebase Crashlytics
8. **Analytics**: Firebase Analytics integration

---

## 📚 Additional Documentation

- `scripts/README.md` - Python scripts documentation

---

## 🤝 Contributing

### Development Setup

1. Clone repository
2. Install Flutter SDK (3.10.4+)
3. Install Node.js (24+) and npm
4. Install Python 3.8+ (for scripts)
5. Configure Firebase project
6. Set up Firebase secrets
7. Run `flutter pub get` and `npm install` in functions

### Code Style

- **Dart**: Follow Flutter style guide (enforced by `analysis_options.yaml`)
- **JavaScript**: Use ESLint configuration (to be added)
- **Python**: Follow PEP 8 style guide
- **Comments**: Document complex logic and decisions

### Testing

- Write tests for new features
- Run existing tests before committing
- Test on multiple platforms (iOS, Android, Web)

---

## 📄 License

[Add your license here]

---

## 👥 Authors

[Add author information here]

---

## 🙏 Acknowledgments

- California Open Data Portal for city data
- Google Maps API for geocoding
- Google Gemini AI for event extraction
- All the libraries and community centers providing toddler activities

---

**Last Updated**: January 2025  
**Version**: 1.0.0  
**Status**: Active Development
