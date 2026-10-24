import 'models/activity.dart';

/// Time buttons in the filter bar and settings.
enum TimeFrame {
  today('Today', 'today'),
  tomorrow('Tomorrow', 'tomorrow'),
  thisWeek('This Week', 'this week'),
  nextWeek('Next Week', 'next week');

  const TimeFrame(this.label, this.phrase);

  /// Button and summary chip label, e.g. "This Week".
  final String label;

  /// Used in sentences, e.g. "No activities found this week."
  final String phrase;
}

/// Age buttons in the filter bar and settings.
enum AgeBand {
  zeroToTwo(0, 2, '0-2 years', '0-2 yrs', '0–2'),
  twoToFour(2, 4, '2-4 years', '2-4 yrs', '2–4');

  const AgeBand(this.min, this.max, this.label, this.chip, this.short);

  final num min;
  final num max;

  /// Button label in the filter panel, e.g. "2-4 years".
  final String label;

  /// Summary chip in the collapsed filter bar, e.g. "2-4 yrs".
  final String chip;

  /// Settings button label, e.g. "2–4".
  final String short;
}

/// Distance buttons in the filter bar and settings: how far from the user (or
/// the picked city) activities are listed.
enum DistanceRange {
  five(5),
  ten(10),
  twenty(20);

  const DistanceRange(this.miles);

  final double miles;

  /// Button and summary chip label, e.g. "10 mi".
  String get label => '${miles.toStringAsFixed(0)} mi';
}

/// The user's filter choices; persisted by SettingsStore.
class ActivityFilters {
  final TimeFrame timeFrame;

  /// null = any age.
  final AgeBand? ageBand;
  final bool indoorOnly;

  /// Only applies when there is a location to measure from.
  final DistanceRange distance;

  const ActivityFilters({
    this.timeFrame = TimeFrame.today,
    this.ageBand,
    this.indoorOnly = false,
    this.distance = DistanceRange.ten,
  });

  ActivityFilters copyWith({
    TimeFrame? timeFrame,
    AgeBand? Function()? ageBand,
    bool? indoorOnly,
    DistanceRange? distance,
  }) => ActivityFilters(
    timeFrame: timeFrame ?? this.timeFrame,
    ageBand: ageBand != null ? ageBand() : this.ageBand,
    indoorOnly: indoorOnly ?? this.indoorOnly,
    distance: distance ?? this.distance,
  );

  /// Labels for the collapsed filter bar, e.g. ["Today", "10 mi", "2-4 yrs",
  /// "Indoor"].
  List<String> get summaryChips => [
    timeFrame.label,
    distance.label,
    if (ageBand != null) ageBand!.chip,
    if (indoorOnly) 'Indoor',
  ];

  bool matches(Activity a, DateTime now) =>
      matchesTimeFrame(a, timeFrame, now) &&
      matchesAge(a, ageBand) &&
      (!indoorOnly || a.isIndoor);
}

/// An activity's age range in years, or null when it is for all ages / unknown.
({num min, num max})? activityAgeYears(Activity a) {
  final years = a.ageRangeYears;
  if (years != null && years.length == 2) return (min: years[0], max: years[1]);
  final m = RegExp(
    r'(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s*(months?|mos?|years?|yrs?)?',
    caseSensitive: false,
  ).firstMatch(a.ageRange);
  if (m == null) return null;
  final inMonths = (m.group(3) ?? '').toLowerCase().startsWith('mo');
  final scale = inMonths ? 12 : 1;
  return (
    min: num.parse(m.group(1)!) / scale,
    max: num.parse(m.group(2)!) / scale,
  );
}

/// Whether an activity suits the age band; all-ages activities always do.
bool matchesAge(Activity a, AgeBand? band) {
  if (band == null) return true;
  final ages = activityAgeYears(a);
  if (ages == null) return true;
  return ages.min < band.max && ages.max > band.min;
}

/// How long an activity stays listed (dimmed, tagged "Ended") after it ends.
const endedGrace = Duration(minutes: 30);

/// Today: starting before midnight and not over (or over less than [endedGrace]
/// ago). Tomorrow: on the calendar day after today. This week: through the end
/// of Sunday. Next week: Monday through Sunday after this week.
bool matchesTimeFrame(Activity a, TimeFrame frame, DateTime now) {
  final nextMonday = DateTime(now.year, now.month, now.day + 8 - now.weekday);
  final from = switch (frame) {
    TimeFrame.tomorrow => DateTime(now.year, now.month, now.day + 1),
    TimeFrame.nextWeek => nextMonday,
    _ => now.subtract(endedGrace),
  };
  final end = a.endTime > a.startTime ? a.endTime : a.startTime + 3600;
  if (end <= from.millisecondsSinceEpoch ~/ 1000) return false;
  final limit = switch (frame) {
    TimeFrame.today => DateTime(now.year, now.month, now.day + 1),
    TimeFrame.tomorrow => DateTime(now.year, now.month, now.day + 2),
    TimeFrame.thisWeek => nextMonday,
    TimeFrame.nextWeek => DateTime(
      nextMonday.year,
      nextMonday.month,
      nextMonday.day + 7,
    ),
  };
  return a.startTime < limit.millisecondsSinceEpoch ~/ 1000;
}

/// Rough driving time for a straight-line distance (~25 mph average with
/// detours), shown as "5 min drive".
int driveMinutes(double miles) => (miles / 25 * 60).round().clamp(1, 999);

const _weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const _months = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', //
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

/// "Today", "Tomorrow", or "Wed, Oct 14".
String formatDay(DateTime start, DateTime now) {
  final today = DateTime(now.year, now.month, now.day);
  final day = DateTime(start.year, start.month, start.day);
  final diff = day.difference(today).inDays;
  if (diff == 0) return 'Today';
  if (diff == 1) return 'Tomorrow';
  return '${_weekdays[start.weekday - 1]}, ${_months[start.month - 1]} ${start.day}';
}

String _clock(DateTime t) {
  final h = t.hour % 12 == 0 ? 12 : t.hour % 12;
  return '$h:${t.minute.toString().padLeft(2, '0')}';
}

String _meridiem(DateTime t) => t.hour < 12 ? 'AM' : 'PM';

/// "3:30–5:00 PM", "11:00 AM–12:00 PM", "10:30 AM" or "All day".
/// Only the start is shown when the end time is an estimate.
String formatTimeRange(Activity a) {
  if (a.isAllDay == true) return 'All day';
  final start = DateTime.fromMillisecondsSinceEpoch(a.startTime * 1000);
  if (a.endTime <= a.startTime || a.durationEstimated) {
    return '${_clock(start)} ${_meridiem(start)}';
  }
  final end = DateTime.fromMillisecondsSinceEpoch(a.endTime * 1000);
  if (_meridiem(start) == _meridiem(end)) {
    return '${_clock(start)}–${_clock(end)} ${_meridiem(end)}';
  }
  return '${_clock(start)} ${_meridiem(start)}–${_clock(end)} ${_meridiem(end)}';
}

/// "Today · 3:30–5:00 PM", or "Through Wed, Nov 4" for a multi-day event
/// (e.g. a library display) that began on an earlier day and is still running.
String formatWhen(Activity a, DateTime now) {
  final start = DateTime.fromMillisecondsSinceEpoch(a.startTime * 1000);
  final today = DateTime(now.year, now.month, now.day);
  if (start.isBefore(today) && a.endTime > a.startTime) {
    final end = DateTime.fromMillisecondsSinceEpoch(a.endTime * 1000);
    return 'Through ${formatDay(end, now)}';
  }
  return '${formatDay(start, now)} · ${formatTimeRange(a)}';
}

/// "45 min", "1 hr", "1 hr 30 min".
String formatMinutes(int minutes) {
  if (minutes < 60) return '$minutes min';
  final h = minutes ~/ 60, m = minutes % 60;
  return m == 0 ? '$h hr' : '$h hr $m min';
}

/// Length label: "30 min", "~30 min" when estimated, or null for all-day events.
String? formatDuration(Activity a) {
  if (a.isAllDay == true) return null;
  final label = formatMinutes(a.durationMinutes);
  return a.durationEstimated ? '~$label' : label;
}

/// Where an activity is relative to now, for the tag above its title.
enum TimeState { upcoming, ongoing, ended }

typedef TimeStatus = ({TimeState state, String label});

/// "Upcoming · in 25 min" (starts within the hour), "Ongoing · 15 min left",
/// or "Ended"; null for all-day events and ones starting more than an hour out.
/// With an estimated end, "Ongoing" says when it started and "Ended" is hedged.
TimeStatus? timeStatus(Activity a, DateTime now) {
  if (a.isAllDay == true) return null;
  final nowSec = now.millisecondsSinceEpoch ~/ 1000;
  if (nowSec >= a.endTime) {
    return (
      state: TimeState.ended,
      label: a.durationEstimated ? 'Likely ended' : 'Ended',
    );
  }
  if (nowSec >= a.startTime) {
    if (a.durationEstimated) {
      final startedMin = ((nowSec - a.startTime) ~/ 60).clamp(1, 999);
      return (
        state: TimeState.ongoing,
        label: 'Ongoing · started ${formatMinutes(startedMin)} ago',
      );
    }
    final leftMin = ((a.endTime - nowSec) / 60).ceil();
    return (
      state: TimeState.ongoing,
      label: 'Ongoing · ${formatMinutes(leftMin)} left',
    );
  }
  final untilMin = ((a.startTime - nowSec) / 60).ceil();
  if (untilMin > 60) return null;
  return (
    state: TimeState.upcoming,
    label: 'Upcoming · in ${formatMinutes(untilMin)}',
  );
}

/// Whether leaving now is worth the drive, so nobody arrives at one that is
/// over. [warn] marks a trip likely to be wasted (or mostly missed).
typedef TripStatus = ({String text, bool warn});

/// Status for a trip started at [now], or null when there is nothing to say
/// (no drive time, you arrive before it starts, all day, or already over).
TripStatus? tripStatus(Activity a, DateTime now, {int? driveMinutes}) {
  if (a.isAllDay == true || driveMinutes == null) return null;
  final nowSec = now.millisecondsSinceEpoch ~/ 1000;
  if (nowSec >= a.endTime) return null;
  final arriveSec = nowSec + driveMinutes * 60;

  if (arriveSec >= a.endTime) {
    return (
      text: a.durationEstimated
          ? 'May be over before you arrive'
          : 'Over before you can get there',
      warn: true,
    );
  }
  final lateMin = ((arriveSec - a.startTime) / 60).ceil();
  if (lateMin <= 0) return null;
  // Missing half of a short storytime is not worth the drive
  final warn = lateMin * 2 >= a.durationMinutes;
  if (nowSec >= a.startTime) {
    final leftMin = ((a.endTime - arriveSec) / 60).ceil();
    return (text: "You'd arrive with ~${formatMinutes(leftMin)} left", warn: warn);
  }
  return (
    text: "You'd arrive ~${formatMinutes(lateMin)} after it starts",
    warn: warn,
  );
}

String _num(num n) =>
    n == n.roundToDouble() ? n.toInt().toString() : n.toStringAsFixed(1);

/// Card label for the age range, e.g. "1–4", "0–18 mo", or "All ages".
String formatAge(Activity a) {
  final ages = activityAgeYears(a);
  if (ages == null) return 'All ages';
  if (ages.max < 2 && ages.max * 12 == (ages.max * 12).roundToDouble()) {
    return '${_num(ages.min * 12)}–${_num(ages.max * 12)} mo';
  }
  return '${_num(ages.min)}–${_num(ages.max)}';
}

/// Words that show a branch name already names its building or place.
final _placeWords = RegExp(
  r'librar|branch|park|depot|hall|room|(resource|community|senior) center',
  caseSensitive: false,
);

/// Short venue for cards. Scrapers store library venues as "System - Branch"
/// (full names geocode better). A county system's branches are in other cities,
/// so "San Mateo County Libraries - Foster City" reads as San Mateo: show the
/// branch, "Foster City Library". A city system ("Hayward Public Library -
/// Downtown Library") already names the city, so it stays as is.
String displayVenue(String venue) {
  final dash = venue.indexOf(' - ');
  if (dash < 0) return venue;
  final system = venue.substring(0, dash);
  final branch = venue.substring(dash + 3).trim();
  if (branch.isEmpty ||
      !RegExp(r'county librar', caseSensitive: false).hasMatch(system)) {
    return venue;
  }
  return _placeWords.hasMatch(branch) ? branch : '$branch Library';
}

/// Placeholder art for an activity without a photo, chosen from its title and tags.
enum ActivityKind {
  music('🎵', 0xFFF3E8FF),
  outdoor('🌳', 0xFFDCFCE7),
  art('🎨', 0xFFFFEDD5),
  science('🔬', 0xFFCCFBF1),
  play('🧸', 0xFFFEF9C3),
  story('📚', 0xFFDBEAFE);

  const ActivityKind(this.emoji, this.background);

  final String emoji;
  final int background;
}

/// Keywords for each kind, in priority order.
const _kindPatterns = [
  (ActivityKind.music, r'music|movement|\bsing\b|song|dance|rhythm|música'),
  (ActivityKind.outdoor, r'\bpark\b|garden|nature|outdoor|playground|hike'),
  (ActivityKind.art, r'\bart\b|craft|paint|colou?r|draw|maker'),
  (ActivityKind.science, r'science|steam|\bstem\b|lego|build'),
  (ActivityKind.story, r'story|stories|book|read|rhyme|lapsit'),
  (ActivityKind.play, r'play|sensory|gym|tumbl|yoga'),
];

/// The title decides first; library tags (often broad, e.g. "Movies, Music &
/// Performances" on a storytime) only when the title matches nothing.
ActivityKind activityKind(Activity a) {
  ActivityKind? match(String text) {
    for (final (kind, pattern) in _kindPatterns) {
      if (RegExp(pattern).hasMatch(text)) return kind;
    }
    return null;
  }

  return match(a.title.toLowerCase()) ??
      (a.isIndoor ? null : ActivityKind.outdoor) ??
      match(a.tags.join(' ').toLowerCase()) ??
      ActivityKind.story;
}
