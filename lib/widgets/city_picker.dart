import 'package:flutter/material.dart';

import '../cities.dart';
import '../theme.dart';
import '../weekly_schedule.dart' show distanceMiles;

/// Bottom sheet for choosing where to look: "Use my location" (the default)
/// or a Bay Area city, with search. Cities are listed nearest first when the
/// device location is known.
///
/// Returns '' for "Use my location", a city name, or null when dismissed.
Future<String?> showCityPicker(
  BuildContext context, {
  String? selectedCity,
  ({double lat, double lng})? device,
  String? deviceLabel,
}) {
  return showModalBottomSheet<String>(
    context: context,
    showDragHandle: true,
    isScrollControlled: true,
    builder: (context) => _CityPicker(
      selectedCity: selectedCity,
      device: device,
      deviceLabel: deviceLabel,
    ),
  );
}

class _CityPicker extends StatefulWidget {
  final String? selectedCity;
  final ({double lat, double lng})? device;
  final String? deviceLabel;

  const _CityPicker({this.selectedCity, this.device, this.deviceLabel});

  @override
  State<_CityPicker> createState() => _CityPickerState();
}

class _CityPickerState extends State<_CityPicker> {
  String _query = '';

  @override
  Widget build(BuildContext context) {
    final device = widget.device;
    final cities = searchCities(_query, from: device);
    // Keep the search field above the keyboard
    return Padding(
      padding: EdgeInsets.only(
        bottom: MediaQuery.viewInsetsOf(context).bottom,
      ),
      child: DraggableScrollableSheet(
        expand: false,
        initialChildSize: 0.75,
        maxChildSize: 0.95,
        builder: (context, controller) => Column(
          children: [
            Padding(
              padding: const EdgeInsets.fromLTRB(20, 0, 20, 12),
              child: TextField(
                onChanged: (v) => setState(() => _query = v),
                textInputAction: TextInputAction.search,
                decoration: InputDecoration(
                  hintText: 'Search Bay Area cities',
                  prefixIcon: const Icon(Icons.search),
                  filled: true,
                  fillColor: AppColors.surfaceMuted,
                  border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(16),
                    borderSide: BorderSide.none,
                  ),
                ),
              ),
            ),
            Expanded(
              child: ListView(
                controller: controller,
                children: [
                  if (_query.trim().isEmpty) ...[
                    ListTile(
                      leading: const Icon(
                        Icons.my_location,
                        color: AppColors.primary,
                      ),
                      title: const Text('Use my location'),
                      subtitle: Text(
                        widget.deviceLabel ?? 'Activities near where you are',
                      ),
                      selected: widget.selectedCity == null,
                      onTap: () => Navigator.pop(context, ''),
                    ),
                    const Divider(height: 1),
                  ],
                  for (final city in cities)
                    ListTile(
                      title: Text(city.label),
                      trailing: device == null
                          ? null
                          : Text(
                              '${distanceMiles(device.lat, device.lng, city.latitude, city.longitude).toStringAsFixed(0)} mi',
                              style: const TextStyle(
                                color: AppColors.textMuted,
                              ),
                            ),
                      selected: widget.selectedCity == city.name,
                      onTap: () => Navigator.pop(context, city.name),
                    ),
                  if (cities.isEmpty)
                    Padding(
                      padding: const EdgeInsets.all(24),
                      child: Text(
                        'No Bay Area city matches "${_query.trim()}".\n'
                        'ToddleGo covers the Bay Area for now.',
                        textAlign: TextAlign.center,
                        style: const TextStyle(color: AppColors.textSecondary),
                      ),
                    ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
