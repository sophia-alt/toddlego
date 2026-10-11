import 'dart:math' as math;

import 'models/weekly_series.dart';

/// Weekday codes in display order, Monday first.
const List<String> weekdayCodes = ['MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'];

const Map<String, String> weekdayNames = {
  'MO': 'Monday',
  'TU': 'Tuesday',
  'WE': 'Wednesday',
  'TH': 'Thursday',
  'FR': 'Friday',
  'SA': 'Saturday',
  'SU': 'Sunday',
};

/// One row of the timetable: a series and its distance from the user, if known.
class TimetableEntry {
  final WeeklySeries series;
  final double? distanceMiles;

  const TimetableEntry(this.series, this.distanceMiles);
}

/// The weekly programs of one weekday, sorted by start time.
class TimetableDay {
  final String byDay;
  final List<TimetableEntry> entries;

  const TimetableDay(this.byDay, this.entries);

  String get name => weekdayNames[byDay] ?? byDay;
}

/// Great-circle distance in miles.
double distanceMiles(double lat1, double lng1, double lat2, double lng2) {
  const earthRadiusMiles = 3958.8;
  double rad(double deg) => deg * math.pi / 180;
  final dLat = rad(lat2 - lat1);
  final dLng = rad(lng2 - lng1);
  final a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(rad(lat1)) *
          math.cos(rad(lat2)) *
          math.sin(dLng / 2) *
          math.sin(dLng / 2);
  return earthRadiusMiles * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
}

/// Groups weekly programs by weekday (Monday first), each day sorted by start time,
/// then by distance.
///
/// Only series with a session from [nowSec] on are included. When [latitude] and
/// [longitude] are given, series farther than [radiusMiles] away, or without
/// coordinates, are left out. Days with no programs are omitted.
List<TimetableDay> buildWeeklyTimetable(
  Iterable<WeeklySeries> series, {
  required int nowSec,
  double? latitude,
  double? longitude,
  double radiusMiles = 10,
}) {
  final byDay = <String, List<TimetableEntry>>{};
  final hasLocation = latitude != null && longitude != null;
  for (final s in series) {
    if (s.lastSessionAt < nowSec) continue; // the program has ended
    double? distance;
    if (hasLocation) {
      if (s.latitude == null || s.longitude == null) continue;
      distance = distanceMiles(latitude, longitude, s.latitude!, s.longitude!);
      if (distance > radiusMiles) continue;
    }
    byDay.putIfAbsent(s.byDay, () => []).add(TimetableEntry(s, distance));
  }

  return [
    for (final code in weekdayCodes)
      if (byDay[code] != null)
        TimetableDay(
          code,
          byDay[code]!..sort((a, b) {
            final byTime = a.series.localStartTime.compareTo(
              b.series.localStartTime,
            );
            if (byTime != 0) return byTime;
            return (a.distanceMiles ?? 0).compareTo(b.distanceMiles ?? 0);
          }),
        ),
  ];
}
