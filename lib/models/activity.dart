import 'package:cloud_firestore/cloud_firestore.dart';

class Activity {
  final String id;
  final String title;
  final String venue;
  final String? description;
  final int startTime; // Unix timestamp
  final int endTime;
  final String ageRange; // "0-2", "2-4", or "All"
  final bool isIndoor;
  final String sourceUrl;
  final bool isFree;
  final bool requiresBooking;
  final String? registrationUrl;
  final double latitude;
  final double longitude;
  final String? type; // one_time, recurring, static
  final bool? isAllDay;

  Activity({
    required this.id,
    required this.title,
    required this.venue,
    this.description,
    required this.startTime,
    required this.endTime,
    required this.ageRange,
    required this.isIndoor,
    required this.sourceUrl,
    required this.isFree,
    required this.requiresBooking,
    this.registrationUrl,
    required this.latitude,
    required this.longitude,
    this.type,
    this.isAllDay,
  });

  // Convert Firestore document to Activity object
  factory Activity.fromFirestore(Map<String, dynamic> data, String id) {
    // Location handling: prefer nested geopoint
    double lat = (data['latitude'] as num?)?.toDouble() ?? 0.0;
    double lon = (data['longitude'] as num?)?.toDouble() ?? 0.0;

    final position = data['position'];
    final location = data['location'];
    GeoPoint? gp;
    if (position is Map<String, dynamic>) {
      final dynamic geo = position['geopoint'];
      if (geo is GeoPoint) gp = geo;
    }
    if (gp == null && location is Map<String, dynamic>) {
      final dynamic geo = location['geopoint'];
      if (geo is GeoPoint) gp = geo;
    }
    if (gp != null) {
      lat = gp.latitude;
      lon = gp.longitude;
    }

    // Timing handling: prefer nested timing
    int start = data['startTime'] ?? 0;
    int end = data['endTime'] ?? 0;
    bool? isAllDay;
    final timing = data['timing'];
    if (timing is Map<String, dynamic>) {
      start = timing['start_time'] ?? start;
      end = timing['end_time'] ?? end;
      isAllDay = timing['is_all_day'] ?? isAllDay;
    }

    return Activity(
      id: id,
      title: data['title'] ?? '',
      venue:
          data['venue'] ??
          (location is Map<String, dynamic> ? (location['name'] ?? '') : ''),
      description: data['description'],
      startTime: start,
      endTime: end,
      ageRange: data['ageRange'] ?? 'All',
      isIndoor: data['isIndoor'] ?? true,
      sourceUrl: data['sourceUrl'] ?? '',
      isFree: data['isFree'] ?? true,
      requiresBooking: data['requiresBooking'] ?? false,
      registrationUrl: data['registrationUrl'],
      latitude: lat,
      longitude: lon,
      type: data['type'],
      isAllDay: isAllDay,
    );
  }
}
