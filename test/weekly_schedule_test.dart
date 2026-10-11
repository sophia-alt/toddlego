import 'package:flutter_test/flutter_test.dart';
import 'package:toddlego/models/weekly_series.dart';
import 'package:toddlego/weekly_schedule.dart';

// Mountain View, the emulator's default location
const double userLat = 37.4220;
const double userLng = -122.0841;
const int now = 1760400000;

WeeklySeries series(
  String id, {
  String byDay = 'WE',
  String time = '10:30',
  double? lat = 37.43,
  double? lng = -122.10,
  int lastSessionAt = now + 7 * 86400,
}) => WeeklySeries(
  id: id,
  title: 'Storytime $id',
  venue: 'Library',
  byDay: byDay,
  interval: 1,
  localStartTime: time,
  durationMinutes: 30,
  lastSessionAt: lastSessionAt,
  latitude: lat,
  longitude: lng,
);

List<String> ids(TimetableDay day) =>
    day.entries.map((e) => e.series.id).toList();

void main() {
  group('WeeklySeries.fromFirestore', () {
    test('reads a series doc written by linkSeries', () {
      final s = WeeklySeries.fromFirestore({
        'title': ' Baby Bounce ',
        'venue': 'Alameda County Library - Dublin',
        'venueId': 'name_abc',
        'byDay': 'WE',
        'interval': 1,
        'localStartTime': '10:30',
        'durationMinutes': 30,
        'ageRange': '0-3 years',
        'latitude': 37.7,
        'longitude': -121.9,
        'lastSessionAt': 1761000000,
        'rrule': 'FREQ=WEEKLY;BYDAY=WE',
      }, 'abc')!;
      expect(s.title, 'Baby Bounce');
      expect(s.byDay, 'WE');
      expect(s.localEndTime, '11:00');
      expect(s.latitude, 37.7);
    });

    test('rejects docs without a weekday or start time', () {
      expect(
        WeeklySeries.fromFirestore({'title': 'x', 'byDay': 'WE'}, 'a'),
        isNull,
      );
      expect(
        WeeklySeries.fromFirestore({
          'title': 'x',
          'byDay': 'WE',
          'localStartTime': '9:30',
        }, 'a'),
        isNull,
      );
    });

    test('end time is unknown without a duration or past midnight', () {
      expect(series('a').localEndTime, '11:00');
      final late = WeeklySeries.fromFirestore({
        'title': 'x',
        'byDay': 'FR',
        'localStartTime': '23:45',
        'durationMinutes': 30,
      }, 'a')!;
      expect(late.localEndTime, isNull);
    });
  });

  group('buildWeeklyTimetable', () {
    test('groups by weekday, Monday first, and sorts by time', () {
      final days = buildWeeklyTimetable([
        series('fri', byDay: 'FR'),
        series('wedLate', byDay: 'WE', time: '16:00'),
        series('mon', byDay: 'MO'),
        series('wedEarly', byDay: 'WE', time: '09:30'),
      ], nowSec: now);
      expect(days.map((d) => d.name), ['Monday', 'Wednesday', 'Friday']);
      expect(ids(days[1]), ['wedEarly', 'wedLate']);
    });

    test('filters by distance when the location is known', () {
      final days = buildWeeklyTimetable(
        [
          series('near'), // ~1 mile away
          series('far', lat: 37.8, lng: -122.27), // Oakland, ~28 miles
          series('noCoords', lat: null, lng: null),
        ],
        nowSec: now,
        latitude: userLat,
        longitude: userLng,
        radiusMiles: 10,
      );
      expect(ids(days.single), ['near']);
      expect(days.single.entries.single.distanceMiles, lessThan(2));
    });

    test('without a location nothing is filtered by distance', () {
      final days = buildWeeklyTimetable([
        series('far', lat: 37.8, lng: -122.27),
        series('noCoords', lat: null, lng: null),
      ], nowSec: now);
      expect(ids(days.single), ['far', 'noCoords']);
      expect(days.single.entries.first.distanceMiles, isNull);
    });

    test('leaves out programs whose last session has passed', () {
      final days = buildWeeklyTimetable([
        series('ended', lastSessionAt: now - 1),
        series('active'),
      ], nowSec: now);
      expect(ids(days.single), ['active']);
    });

    test('same start time sorts by distance', () {
      final days = buildWeeklyTimetable(
        [series('farther', lat: 37.50), series('closer', lat: 37.425)],
        nowSec: now,
        latitude: userLat,
        longitude: userLng,
      );
      expect(ids(days.single), ['closer', 'farther']);
    });
  });

  test('distanceMiles matches a known distance', () {
    // San Francisco City Hall to Oakland City Hall is about 8.4 miles
    expect(
      distanceMiles(37.7793, -122.4193, 37.8053, -122.2725),
      closeTo(8.2, 0.4),
    );
  });
}
