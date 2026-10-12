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
  bool estimated = false,
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
    durationEstimated: estimated,
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

    test('tomorrow: the next calendar day only', () {
      final morning = activity(start: DateTime(2026, 10, 14, 10, 0));
      expect(matchesTimeFrame(morning, TimeFrame.tomorrow, now), isTrue);
      expect(matchesTimeFrame(activity(), TimeFrame.tomorrow, now), isFalse);
      final dayAfter = activity(start: DateTime(2026, 10, 15, 10, 0));
      expect(matchesTimeFrame(dayAfter, TimeFrame.tomorrow, now), isFalse);
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

    test('multi-day events already running show their last day', () {
      final display = activity(
        start: DateTime(2026, 10, 10, 0, 0),
        length: const Duration(days: 25, hours: 23, minutes: 59),
        allDay: true,
      );
      expect(formatWhen(display, now), 'Through Wed, Nov 4');
      expect(matchesTimeFrame(display, TimeFrame.today, now), isTrue);
      // An all-day event today still shows today
      final today = activity(
        start: DateTime(2026, 10, 13, 0, 0),
        length: const Duration(hours: 23, minutes: 59),
        allDay: true,
      );
      expect(formatWhen(today, now), 'Today · All day');
    });

    test('age labels', () {
      expect(formatAge(activity(years: [1, 4])), '1–4');
      expect(formatAge(activity(ageRange: '0-18 months')), '0–18 mo');
      expect(formatAge(activity(ageRange: '18-36 months')), '1.5–3');
      expect(formatAge(activity()), 'All ages');
    });

    test('county library venues show the branch; city ones stay whole', () {
      expect(
        displayVenue('San Mateo County Libraries - Foster City'),
        'Foster City Library',
      );
      expect(displayVenue('Alameda County Library - Dublin'), 'Dublin Library');
      expect(
        displayVenue(
          'Alameda County Library - Wilma Chan Family Resource Center',
        ),
        'Wilma Chan Family Resource Center',
      );
      expect(
        displayVenue('Hayward Public Library - Downtown Library'),
        'Hayward Public Library - Downtown Library',
      );
      expect(
        displayVenue('Livermore Public Library - Civic Center'),
        'Livermore Public Library - Civic Center',
      );
      expect(displayVenue('Novato Library, Novato'), 'Novato Library, Novato');
      expect(displayVenue('Kids Gym - Downtown'), 'Kids Gym - Downtown');
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

    test('search ignores case and a trailing ", CA"', () {
      expect(searchCities('san mateo, ca').map((c) => c.name), ['San Mateo']);
      expect(searchCities('  OAK ').map((c) => c.name), ['Oakland']);
      expect(searchCities('').length, bayAreaCities.length);
      expect(searchCities('Atlantis'), isEmpty);
    });

    test('search lists nearest cities first when the location is known', () {
      final nearSanMateo = searchCities('', from: (lat: 37.5630, lng: -122.3255));
      expect(nearSanMateo.first.name, 'San Mateo');
      final sans = searchCities('san', from: (lat: 37.3382, lng: -121.8863));
      expect(sans.first.name, 'San Jose');
    });

    test('lookup by name', () {
      expect(cityNamed('Oakland')?.label, 'Oakland, CA');
      expect(cityNamed('Atlantis'), isNull);
      expect(cityNamed(null), isNull);
    });
  });

  group('duration', () {
    test('listed lengths show the range; estimates show only the start', () {
      expect(formatTimeRange(activity()), '10:30–11:00 AM');
      expect(formatDuration(activity()), '30 min');
      final guessed = activity(estimated: true);
      expect(formatTimeRange(guessed), '10:30 AM');
      expect(formatDuration(guessed), '~30 min');
      expect(formatDuration(activity(allDay: true)), isNull);
      expect(formatMinutes(90), '1 hr 30 min');
      expect(formatMinutes(60), '1 hr');
    });
  });

  group('trip status', () {
    // now is 9:00; the default activity runs 10:30–11:00
    test('nothing to say when you can get there in time', () {
      expect(tripStatus(activity(), now, driveMinutes: 20), isNull);
    });

    test('warns when it is over before you arrive', () {
      final running = activity(start: DateTime(2026, 10, 13, 8, 45));
      final s = tripStatus(running, now, driveMinutes: 20)!;
      expect(s.warn, isTrue);
      expect(s.text, 'Over before you can get there');
      final guessed = activity(
        start: DateTime(2026, 10, 13, 8, 45),
        estimated: true,
      );
      expect(
        tripStatus(guessed, now, driveMinutes: 20)!.text,
        'May be over before you arrive',
      );
    });

    test('already running: says how much is left when you arrive', () {
      final running = activity(start: DateTime(2026, 10, 13, 8, 45));
      final s = tripStatus(running, now, driveMinutes: 5)!;
      expect(s.text, "You'd arrive with ~10 min left");
      expect(s.warn, isTrue);
      expect(tripStatus(running, now), isNull, reason: 'no drive time');
    });

    test('arriving late warns once half of it would be missed', () {
      final soon = activity(start: DateTime(2026, 10, 13, 9, 10));
      final slightly = tripStatus(soon, now, driveMinutes: 15)!;
      expect(slightly.text, "You'd arrive ~5 min after it starts");
      expect(slightly.warn, isFalse);
      expect(tripStatus(soon, now, driveMinutes: 25)!.warn, isTrue);
    });
  });

  group('time tag', () {
    // now is 9:00
    test('upcoming only within the hour', () {
      final soon = activity(start: DateTime(2026, 10, 13, 9, 25));
      expect(timeStatus(soon, now), (
        state: TimeState.upcoming,
        label: 'Upcoming · in 25 min',
      ));
      final inAnHour = activity(start: DateTime(2026, 10, 13, 10, 0));
      expect(timeStatus(inAnHour, now)!.label, 'Upcoming · in 1 hr');
      expect(timeStatus(activity(), now), isNull, reason: '10:30 is 90 min out');
    });

    test('ongoing shows time left, or when it started if the end is a guess', () {
      final running = activity(start: DateTime(2026, 10, 13, 8, 45));
      expect(timeStatus(running, now), (
        state: TimeState.ongoing,
        label: 'Ongoing · 15 min left',
      ));
      final guessed = activity(
        start: DateTime(2026, 10, 13, 8, 45),
        estimated: true,
      );
      expect(timeStatus(guessed, now)!.label, 'Ongoing · started 15 min ago');
    });

    test('ended stays listed for a while, hedged when the end was a guess', () {
      final justOver = activity(start: DateTime(2026, 10, 13, 8, 15));
      expect(timeStatus(justOver, now), (
        state: TimeState.ended,
        label: 'Ended',
      ));
      expect(matchesTimeFrame(justOver, TimeFrame.today, now), isTrue);
      final guessed = activity(
        start: DateTime(2026, 10, 13, 8, 15),
        estimated: true,
      );
      expect(timeStatus(guessed, now)!.label, 'Likely ended');
      final longOver = activity(start: DateTime(2026, 10, 13, 8, 0));
      expect(
        matchesTimeFrame(longOver, TimeFrame.today, now),
        isFalse,
        reason: 'ended 30 min ago',
      );
      expect(timeStatus(activity(allDay: true), now), isNull);
    });
  });
}
