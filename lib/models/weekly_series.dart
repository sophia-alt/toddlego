/// A weekly program (e.g. "Toddler Storytime every Wednesday at 10:30") from the
/// `series` collection, written by the Cloud Functions from scraped sessions.
class WeeklySeries {
  final String id;
  final String title;
  final String venue;
  final String? venueId;

  /// RRULE weekday code: MO, TU, WE, TH, FR, SA or SU.
  final String byDay;

  /// 1 = every week, 2 = every other week.
  final int interval;

  /// Local start time, "HH:mm".
  final String localStartTime;
  final int durationMinutes;

  /// True when the source listed no end time and [durationMinutes] is a typical length.
  final bool durationEstimated;
  final String? ageRange;
  final double? latitude;
  final double? longitude;

  /// Start of the latest known session (Unix seconds).
  final int lastSessionAt;
  final String? sourceUrl;

  const WeeklySeries({
    required this.id,
    required this.title,
    required this.venue,
    required this.byDay,
    required this.interval,
    required this.localStartTime,
    required this.durationMinutes,
    this.durationEstimated = false,
    required this.lastSessionAt,
    this.venueId,
    this.ageRange,
    this.latitude,
    this.longitude,
    this.sourceUrl,
  });

  /// Returns null for docs missing the fields a timetable row needs.
  static WeeklySeries? fromFirestore(Map<String, dynamic> data, String id) {
    final title = (data['title'] as String?)?.trim() ?? '';
    final byDay = data['byDay'] as String?;
    final localStartTime = data['localStartTime'] as String?;
    if (title.isEmpty ||
        byDay == null ||
        localStartTime == null ||
        !RegExp(r'^\d{2}:\d{2}$').hasMatch(localStartTime)) {
      return null;
    }
    return WeeklySeries(
      id: id,
      title: title,
      venue: (data['venue'] as String?)?.trim() ?? '',
      venueId: data['venueId'] as String?,
      byDay: byDay,
      interval: (data['interval'] as num?)?.toInt() ?? 1,
      localStartTime: localStartTime,
      durationMinutes: (data['durationMinutes'] as num?)?.toInt() ?? 0,
      // Older series have no flag; their unknown lengths were stored as 60 min
      durationEstimated:
          data['durationEstimated'] as bool? ?? data['durationMinutes'] == 60,
      ageRange: data['ageRange'] as String?,
      latitude: (data['latitude'] as num?)?.toDouble(),
      longitude: (data['longitude'] as num?)?.toDouble(),
      lastSessionAt: (data['lastSessionAt'] as num?)?.toInt() ?? 0,
      sourceUrl: data['sourceUrl'] as String?,
    );
  }

  /// Local end time "HH:mm", or null when the duration is unknown.
  String? get localEndTime {
    if (durationMinutes <= 0) return null;
    final parts = localStartTime.split(':').map(int.parse).toList();
    final total = parts[0] * 60 + parts[1] + durationMinutes;
    if (total >= 24 * 60) return null;
    return '${(total ~/ 60).toString().padLeft(2, '0')}:'
        '${(total % 60).toString().padLeft(2, '0')}';
  }
}
