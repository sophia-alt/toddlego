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
  });

  // Convert Firestore document to Activity object
  factory Activity.fromFirestore(Map<String, dynamic> data, String id) {
    return Activity(
      id: id,
      title: data['title'] ?? '',
      venue: data['venue'] ?? '',
      description: data['description'],
      startTime: data['startTime'] ?? 0,
      endTime: data['endTime'] ?? 0,
      ageRange: data['ageRange'] ?? 'All',
      isIndoor: data['isIndoor'] ?? true,
      sourceUrl: data['sourceUrl'] ?? '',
      isFree: data['isFree'] ?? true,
      requiresBooking: data['requiresBooking'] ?? false,
      registrationUrl: data['registrationUrl'],
      latitude: (data['latitude'] as num).toDouble(),
      longitude: (data['longitude'] as num).toDouble(),
    );
  }
}
