import 'package:flutter/material.dart';

import '../cities.dart';
import '../filters.dart';
import '../settings_store.dart';
import '../theme.dart';
import '../widgets/city_picker.dart';
import '../widgets/components.dart';

/// Time frame, age range, indoor preference and city. Changes apply immediately
/// through [onChanged]; the home screen saves them.
class SettingsScreen extends StatefulWidget {
  final AppSettings settings;

  /// Shown under "Change city" when no city is picked, e.g. "San Mateo, CA".
  final String? deviceLocationLabel;

  /// The device position, so the city list can show nearest cities first.
  final ({double lat, double lng})? deviceLocation;
  final ValueChanged<AppSettings> onChanged;

  const SettingsScreen({
    super.key,
    required this.settings,
    required this.onChanged,
    this.deviceLocationLabel,
    this.deviceLocation,
  });

  @override
  State<SettingsScreen> createState() => _SettingsScreenState();
}

class _SettingsScreenState extends State<SettingsScreen> {
  late AppSettings _settings = widget.settings;

  void _update(AppSettings next) {
    setState(() => _settings = next);
    widget.onChanged(next);
  }

  void _updateFilters(ActivityFilters filters) =>
      _update(_settings.copyWith(filters: filters));

  Future<void> _pickCity() async {
    final picked = await showCityPicker(
      context,
      selectedCity: _settings.cityName,
      device: widget.deviceLocation,
      deviceLabel: widget.deviceLocationLabel,
    );
    if (picked == null) return;
    _update(_settings.copyWith(cityName: () => picked.isEmpty ? null : picked));
  }

  @override
  Widget build(BuildContext context) {
    final f = _settings.filters;
    final current =
        cityNamed(_settings.cityName)?.label ??
        widget.deviceLocationLabel ??
        'your location';

    return Scaffold(
      appBar: AppBar(title: const Text('Settings')),
      body: ListView(
        padding: const EdgeInsets.fromLTRB(24, 24, 24, 32),
        children: [
          const _Heading('Time Frame'),
          SolidChoice<TimeFrame>(
            options: const [
              (TimeFrame.today, 'Today'),
              (TimeFrame.thisWeek, 'This Week'),
            ],
            selected: f.timeFrame,
            onSelected: (v) => _updateFilters(f.copyWith(timeFrame: v)),
          ),
          const SizedBox(height: 32),
          const _Heading('Age Range'),
          SolidChoice<AgeBand>(
            options: [for (final b in AgeBand.values) (b, b.short)],
            selected: f.ageBand,
            // Tapping the selected range again shows all ages
            onSelected: (v) => _updateFilters(
              f.copyWith(ageBand: () => v == f.ageBand ? null : v),
            ),
          ),
          const SizedBox(height: 32),
          Row(
            children: [
              const Expanded(
                child: Text(
                  'Prefer indoor activities',
                  style: TextStyle(fontSize: 18, fontWeight: FontWeight.w500),
                ),
              ),
              Switch(
                value: f.indoorOnly,
                activeTrackColor: AppColors.primary,
                onChanged: (v) => _updateFilters(f.copyWith(indoorOnly: v)),
              ),
            ],
          ),
          const SizedBox(height: 40),
          const _Heading('Location'),
          Material(
            color: AppColors.surfaceMuted,
            borderRadius: BorderRadius.circular(16),
            child: InkWell(
              borderRadius: BorderRadius.circular(16),
              onTap: _pickCity,
              child: const SizedBox(
                height: 56,
                width: double.infinity,
                child: Padding(
                  padding: EdgeInsets.symmetric(horizontal: 20),
                  child: Align(
                    alignment: Alignment.centerLeft,
                    child: Text(
                      'Change city',
                      style: TextStyle(
                        fontSize: 18,
                        color: AppColors.textSecondary,
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
          const SizedBox(height: 10),
          Text(
            'Currently: $current',
            style: const TextStyle(color: AppColors.textSecondary),
          ),
        ],
      ),
    );
  }
}

class _Heading extends StatelessWidget {
  final String text;

  const _Heading(this.text);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 14),
      child: Text(
        text,
        style: const TextStyle(fontSize: 18, fontWeight: FontWeight.w500),
      ),
    );
  }
}
