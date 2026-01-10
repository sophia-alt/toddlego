import 'package:cloud_firestore/cloud_firestore.dart';

/// Represents a toddler activity/event with location, timing, and details.
///
/// Activities are fetched from Firestore and displayed in the app.
/// Supports both flat and nested data structures for backward compatibility.
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
    try {
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
      int start = (data['startTime'] as num?)?.toInt() ?? 0;
      int end = (data['endTime'] as num?)?.toInt() ?? 0;
      bool? isAllDay;
      final timing = data['timing'];
      if (timing is Map<String, dynamic>) {
        final startTime = timing['start_time'];
        final endTime = timing['end_time'];
        if (startTime is num) start = startTime.toInt();
        if (endTime is num) end = endTime.toInt();
        isAllDay = timing['is_all_day'] as bool?;
      }

      // Validate required fields
      final title = (data['title'] as String?)?.trim() ?? '';
      final venue =
          (data['venue'] as String?)?.trim() ??
          (location is Map<String, dynamic>
              ? (location['name'] as String?)?.trim() ?? ''
              : '');

      if (title.isEmpty || venue.isEmpty) {
        throw FormatException(
          'Activity missing required fields: title="$title", venue="$venue"',
        );
      }

      return Activity(
        id: id,
        title: title,
        venue: venue,
        description: data['description'] as String?,
        startTime: start,
        endTime: end,
        ageRange: (data['ageRange'] as String?) ?? 'All',
        isIndoor: data['isIndoor'] as bool? ?? true,
        sourceUrl: (data['sourceUrl'] as String?) ?? '',
        isFree: data['isFree'] as bool? ?? true,
        requiresBooking: data['requiresBooking'] as bool? ?? false,
        registrationUrl: data['registrationUrl'] as String?,
        latitude: lat,
        longitude: lon,
        type: data['type'] as String?,
        isAllDay: isAllDay,
      );
    } catch (e) {
      // Log error and return a minimal valid activity to prevent app crash
      // In production, you might want to log this to a crash reporting service
      print('Error parsing Activity from Firestore (id: $id): $e');
      rethrow; // Re-throw to let caller handle it
    }
  }
}
