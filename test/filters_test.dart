import 'package:flutter_test/flutter_test.dart';
import 'package:toddlego/cities.dart';
import 'package:toddlego/filters.dart';
import 'package:toddlego/models/activity.dart';

// A Tuesday morning; times below are local (DateTime without a zone).
final now = DateTime(2026, 10, 13, 9, 0);
int sec(DateTime t) => t.millisecondsSinceEpoch ~/ 1000;

Activity activity({
  DateTime? start,
  Duration length = const Duration(minutes: 30),
  String ageRange = 'All',
  List<num>? years,
  bool indoor = true,
  bool? allDay,
  String title = 'Toddler Storytime',
  List<String> tags = const [],
}) {
  final s = start ?? DateTime(2026, 10, 13, 10, 30);
  return Activity(
    id: 'a',
    title: title,
    venue: 'Library',
    startTime: sec(s),
    endTime: sec(s.add(length)),
    ageRange: ageRange,
    ageRangeYears: years,
    isIndoor: indoor,
    isAllDay: allDay,
    sourceUrl: '',
    isFree: true,
    requiresBooking: false,
    latitude: 37.5,
    longitude: -122.3,
    tags: tags,
  );
}

void main() {
  group('time frame', () {
    test('today: later today, or still running', () {
      expect(matchesTimeFrame(activity(), TimeFrame.today, now), isTrue);
      final running = activity(start: DateTime(2026, 10, 13, 8, 45));
      expect(matchesTimeFrame(running, TimeFrame.today, now), isTrue);
      final over = activity(start: DateTime(2026, 10, 13, 7, 0));
      expect(matchesTimeFrame(over, TimeFrame.today, now), isFalse);
      final tomorrow = activity(start: DateTime(2026, 10, 14, 10, 0));
      expect(matchesTimeFrame(tomorrow, TimeFrame.today, now), isFalse);
    });

    test('this week: the next 7 days', () {
      final sunday = activity(start: DateTime(2026, 10, 19, 10, 0));
      expect(matchesTimeFrame(sunday, TimeFrame.thisWeek, now), isTrue);
      final nextTuesday = activity(start: DateTime(2026, 10, 20, 10, 0));
      expect(matchesTimeFrame(nextTuesday, TimeFrame.thisWeek, now), isFalse);
    });
  });

  group('age', () {
    test('uses age_range years, then parses the label', () {
      expect(activityAgeYears(activity(years: [0, 3])), (min: 0, max: 3));
      expect(activityAgeYears(activity(ageRange: '18-36 months')), (
        min: 1.5,
        max: 3,
      ));
      expect(activityAgeYears(activity(ageRange: '3-5 years')), (
        min: 3,
        max: 5,
      ));
      expect(activityAgeYears(activity(ageRange: 'All')), isNull);
    });

    test('bands match overlapping ranges; all-ages always match', () {
      expect(matchesAge(activity(years: [0, 3]), AgeBand.twoToFour), isTrue);
      expect(matchesAge(activity(years: [3, 5]), AgeBand.zeroToTwo), isFalse);
      expect(
        matchesAge(activity(ageRange: '0-18 months'), AgeBand.twoToFour),
        isFalse,
      );
      expect(matchesAge(activity(), AgeBand.zeroToTwo), isTrue);
      expect(matchesAge(activity(years: [3, 5]), null), isTrue);
    });
  });

  test('indoor only drops outdoor activities', () {
    const f = ActivityFilters(indoorOnly: true);
    expect(f.matches(activity(), now), isTrue);
    expect(f.matches(activity(indoor: false), now), isFalse);
  });

  test('summary chips reflect the filters', () {
    expect(const ActivityFilters().summaryChips, ['Today']);
    expect(
      const ActivityFilters(
        timeFrame: TimeFrame.today,
        ageBand: AgeBand.twoToFour,
        indoorOnly: true,
      ).summaryChips,
      ['Today', '2-4 yrs', 'Indoor'],
    );
  });

  group('formatting', () {
    test('day labels', () {
      expect(formatDay(DateTime(2026, 10, 13, 15), now), 'Today');
      expect(formatDay(DateTime(2026, 10, 14, 9), now), 'Tomorrow');
      expect(formatDay(DateTime(2026, 10, 16, 9), now), 'Fri, Oct 16');
    });

    test('time ranges share AM/PM when they can', () {
      expect(
        formatTimeRange(
          activity(
            start: DateTime(2026, 10, 13, 15, 30),
            length: const Duration(minutes: 90),
          ),
        ),
        '3:30–5:00 PM',
      );
      expect(
        formatTimeRange(
          activity(
            start: DateTime(2026, 10, 13, 11),
            length: const Duration(hours: 1),
          ),
        ),
        '11:00 AM–12:00 PM',
      );
      expect(formatTimeRange(activity(allDay: true)), 'All day');
      expect(formatWhen(activity(), now), 'Today · 10:30–11:00 AM');
    });

    test('age labels', () {
      expect(formatAge(activity(years: [1, 4])), '1–4');
      expect(formatAge(activity(ageRange: '0-18 months')), '0–18 mo');
      expect(formatAge(activity(ageRange: '18-36 months')), '1.5–3');
      expect(formatAge(activity()), 'All ages');
    });

    test('drive time from distance', () {
      expect(driveMinutes(2), 5);
      expect(driveMinutes(0.1), 1);
    });
  });

  test('placeholder art follows the activity kind', () {
    expect(
      activityKind(activity(title: 'Music & Movement')),
      ActivityKind.music,
    );
    expect(
      activityKind(activity(title: 'Storytime at Hagemann Park')),
      ActivityKind.outdoor,
    );
    expect(activityKind(activity(title: 'Family Coloring')), ActivityKind.art);
    expect(
      activityKind(activity(title: 'Sensory Play Hour')),
      ActivityKind.play,
    );
    expect(
      activityKind(activity(title: 'Baby Bounce', tags: ['Storytime'])),
      ActivityKind.story,
    );
    // The title wins over broad library tags
    expect(
      activityKind(
        activity(
          title: 'Family Storytime With Stay and Play',
          tags: ['Movies, Music & Performances'],
        ),
      ),
      ActivityKind.story,
    );
    expect(
      activityKind(
        activity(title: 'Celebrate Navratri', tags: ['Music & Dance']),
      ),
      ActivityKind.music,
    );
    expect(
      activityKind(activity(title: 'Celebrate Navratri', indoor: false)),
      ActivityKind.outdoor,
    );
  });

  group('cities', () {
    test('nearest city to a point in the Bay Area', () {
      expect(nearestCity(37.5630, -122.3255)?.name, 'San Mateo');
      expect(nearestCity(37.4220, -122.0841)?.name, 'Mountain View');
    });

    test('no city far outside the Bay Area', () {
      expect(nearestCity(34.0522, -118.2437), isNull); // Los Angeles
    });

    test('lookup by name', () {
      expect(cityNamed('Oakland')?.label, 'Oakland, CA');
      expect(cityNamed('Atlantis'), isNull);
      expect(cityNamed(null), isNull);
    });
  });
}
