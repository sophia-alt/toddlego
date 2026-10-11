import 'package:shared_preferences/shared_preferences.dart';

import 'filters.dart';

/// Filters and location choice, kept across app launches.
class AppSettings {
  final ActivityFilters filters;

  /// A city picked in Settings, or null to use the device location.
  final String? cityName;

  const AppSettings({this.filters = const ActivityFilters(), this.cityName});

  AppSettings copyWith({
    ActivityFilters? filters,
    String? Function()? cityName,
  }) => AppSettings(
    filters: filters ?? this.filters,
    cityName: cityName != null ? cityName() : this.cityName,
  );
}

class SettingsStore {
  static const _timeFrame = 'filters.timeFrame';
  static const _ageBand = 'filters.ageBand';
  static const _indoorOnly = 'filters.indoorOnly';
  static const _city = 'location.city';

  static Future<AppSettings> load() async {
    final prefs = await SharedPreferences.getInstance();
    T? byName<T extends Enum>(List<T> values, String? name) {
      for (final v in values) {
        if (v.name == name) return v;
      }
      return null;
    }

    return AppSettings(
      filters: ActivityFilters(
        timeFrame:
            byName(TimeFrame.values, prefs.getString(_timeFrame)) ??
            TimeFrame.today,
        ageBand: byName(AgeBand.values, prefs.getString(_ageBand)),
        indoorOnly: prefs.getBool(_indoorOnly) ?? false,
      ),
      cityName: prefs.getString(_city),
    );
  }

  static Future<void> save(AppSettings settings) async {
    final prefs = await SharedPreferences.getInstance();
    final f = settings.filters;
    await prefs.setString(_timeFrame, f.timeFrame.name);
    if (f.ageBand == null) {
      await prefs.remove(_ageBand);
    } else {
      await prefs.setString(_ageBand, f.ageBand!.name);
    }
    await prefs.setBool(_indoorOnly, f.indoorOnly);
    if (settings.cityName == null) {
      await prefs.remove(_city);
    } else {
      await prefs.setString(_city, settings.cityName!);
    }
  }
}
