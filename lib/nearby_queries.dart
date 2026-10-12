import 'package:cloud_firestore/cloud_firestore.dart';

import 'geohash.dart';

const double metersPerMile = 1609.344;

typedef Docs = List<QueryDocumentSnapshot<Map<String, dynamic>>>;

/// Runs one query per geohash range covering the circle and merges the results
/// (ranges can overlap at their edges). Results may lie slightly outside the
/// radius; callers filter by exact distance.
Future<Docs> _byGeohashRanges(
  double latitude,
  double longitude,
  double radiusMiles,
  Query<Map<String, dynamic>> Function(String start, String end) query,
) async {
  final bounds = geohashQueryBounds(
    latitude,
    longitude,
    radiusMiles * metersPerMile,
  );
  final snaps = await Future.wait([
    for (final (start, end) in bounds) query(start, end).get(),
  ]);
  final byId = <String, QueryDocumentSnapshot<Map<String, dynamic>>>{};
  for (final snap in snaps) {
    for (final doc in snap.docs) {
      byId[doc.id] = doc;
    }
  }
  return byId.values.toList();
}

/// Activities near a point that are still running at [fromSec] and start
/// before [toSec]. Multi-day events that began earlier are included.
///
/// Needs the composite index (geohash, endTime, startTime) in
/// firestore.indexes.json.
Future<Docs> fetchNearbyActivities(
  FirebaseFirestore db, {
  required double latitude,
  required double longitude,
  required double radiusMiles,
  required int fromSec,
  required int toSec,
}) {
  return _byGeohashRanges(latitude, longitude, radiusMiles, (start, end) {
    return db
        .collection('activities')
        .where('geohash', isGreaterThanOrEqualTo: start)
        .where('geohash', isLessThanOrEqualTo: end)
        .where('endTime', isGreaterThan: fromSec)
        .where('startTime', isLessThan: toSec)
        // Explicit first ordering; Firestore then orders by endTime, startTime
        .orderBy('geohash');
  });
}

/// Activities anywhere starting in [fromSec, toSec): used when the user has no
/// location and no city, so there is nothing to filter by distance.
Future<Docs> fetchActivitiesStartingBetween(
  FirebaseFirestore db, {
  required int fromSec,
  required int toSec,
}) async {
  final snap = await db
      .collection('activities')
      .where('startTime', isGreaterThanOrEqualTo: fromSec)
      .where('startTime', isLessThan: toSec)
      .orderBy('startTime')
      .get();
  return snap.docs;
}

/// Weekly series near a point with a session from [nowSec] on.
///
/// Needs the composite index (geohash, lastSessionAt) in firestore.indexes.json.
Future<Docs> fetchNearbySeries(
  FirebaseFirestore db, {
  required double latitude,
  required double longitude,
  required double radiusMiles,
  required int nowSec,
}) {
  return _byGeohashRanges(latitude, longitude, radiusMiles, (start, end) {
    return db
        .collection('series')
        .where('geohash', isGreaterThanOrEqualTo: start)
        .where('geohash', isLessThanOrEqualTo: end)
        .where('lastSessionAt', isGreaterThanOrEqualTo: nowSec)
        .orderBy('geohash');
  });
}
