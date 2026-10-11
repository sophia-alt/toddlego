import 'models/activity.dart';

/// "Today" or "This Week" in the filter bar and settings.
enum TimeFrame { today, thisWeek }

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

/// The user's filter choices; persisted by SettingsStore.
class ActivityFilters {
  final TimeFrame timeFrame;

  /// null = any age.
  final AgeBand? ageBand;
  final bool indoorOnly;

  const ActivityFilters({
    this.timeFrame = TimeFrame.today,
    this.ageBand,
    this.indoorOnly = false,
  });

  ActivityFilters copyWith({
    TimeFrame? timeFrame,
    AgeBand? Function()? ageBand,
    bool? indoorOnly,
  }) => ActivityFilters(
    timeFrame: timeFrame ?? this.timeFrame,
    ageBand: ageBand != null ? ageBand() : this.ageBand,
    indoorOnly: indoorOnly ?? this.indoorOnly,
  );

  /// Labels for the collapsed filter bar, e.g. ["Today", "2-4 yrs", "Indoor"].
  List<String> get summaryChips => [
    timeFrame == TimeFrame.today ? 'Today' : 'This Week',
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

/// Today: not yet over and starting before midnight. This week: the next 7 days.
bool matchesTimeFrame(Activity a, TimeFrame frame, DateTime now) {
  final nowSec = now.millisecondsSinceEpoch ~/ 1000;
  final end = a.endTime > a.startTime ? a.endTime : a.startTime + 3600;
  if (end <= nowSec) return false;
  final limit = frame == TimeFrame.today
      ? DateTime(now.year, now.month, now.day + 1)
      : DateTime(now.year, now.month, now.day + 7);
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
String formatTimeRange(Activity a) {
  if (a.isAllDay == true) return 'All day';
  final start = DateTime.fromMillisecondsSinceEpoch(a.startTime * 1000);
  if (a.endTime <= a.startTime) return '${_clock(start)} ${_meridiem(start)}';
  final end = DateTime.fromMillisecondsSinceEpoch(a.endTime * 1000);
  if (_meridiem(start) == _meridiem(end)) {
    return '${_clock(start)}–${_clock(end)} ${_meridiem(end)}';
  }
  return '${_clock(start)} ${_meridiem(start)}–${_clock(end)} ${_meridiem(end)}';
}

/// "Today · 3:30–5:00 PM"
String formatWhen(Activity a, DateTime now) {
  final start = DateTime.fromMillisecondsSinceEpoch(a.startTime * 1000);
  return '${formatDay(start, now)} · ${formatTimeRange(a)}';
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

ActivityKind activityKind(Activity a) {
  final text = '${a.title} ${a.tags.join(' ')}'.toLowerCase();
  bool has(String pattern) => RegExp(pattern).hasMatch(text);
  if (has(r'music|movement|sing|song|dance|rhythm|música')) {
    return ActivityKind.music;
  }
  if (!a.isIndoor || has(r'park|garden|nature|outdoor|playground|hike')) {
    return ActivityKind.outdoor;
  }
  if (has(r'\bart\b|craft|paint|color|draw|maker')) return ActivityKind.art;
  if (has(r'science|steam|stem|lego|build')) return ActivityKind.science;
  if (has(r'play|sensory|gym|tumbl|yoga')) return ActivityKind.play;
  return ActivityKind.story;
}
