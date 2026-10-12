import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';

import 'filters.dart';
import 'models/weekly_series.dart';
import 'nearby_queries.dart';
import 'weekly_schedule.dart';

/// "Every week" timetable: recurring programs near the user, grouped by weekday.
class WeeklyScheduleScreen extends StatefulWidget {
  /// User location; when null the timetable is not filtered by distance.
  final double? latitude;
  final double? longitude;
  final double radiusMiles;

  const WeeklyScheduleScreen({
    super.key,
    this.latitude,
    this.longitude,
    required this.radiusMiles,
  });

  @override
  State<WeeklyScheduleScreen> createState() => _WeeklyScheduleScreenState();
}

class _WeeklyScheduleScreenState extends State<WeeklyScheduleScreen> {
  late Future<Docs> _seriesFuture;

  @override
  void initState() {
    super.initState();
    _seriesFuture = _loadSeries();
  }

  /// Series with an upcoming session: around the user when the location is
  /// known (geohash queries), otherwise all of them.
  Future<Docs> _loadSeries() async {
    final nowSec = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    final db = FirebaseFirestore.instance;
    final lat = widget.latitude, lng = widget.longitude;
    if (lat != null && lng != null) {
      return fetchNearbySeries(
        db,
        latitude: lat,
        longitude: lng,
        radiusMiles: widget.radiusMiles,
        nowSec: nowSec,
      );
    }
    final snap = await db
        .collection('series')
        .where('lastSessionAt', isGreaterThanOrEqualTo: nowSec)
        .get();
    return snap.docs;
  }

  @override
  Widget build(BuildContext context) {
    final filtered = widget.latitude != null && widget.longitude != null;
    return Scaffold(
      appBar: AppBar(title: const Text('Every Week')),
      body: FutureBuilder<Docs>(
        future: _seriesFuture,
        builder: (context, snapshot) {
          if (snapshot.hasError) {
            return Center(
              child: Padding(
                padding: const EdgeInsets.all(24),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const Icon(
                      Icons.error_outline,
                      size: 48,
                      color: Colors.red,
                    ),
                    const SizedBox(height: 12),
                    Text(
                      'Could not load the weekly schedule.\n${snapshot.error}',
                      textAlign: TextAlign.center,
                    ),
                    const SizedBox(height: 12),
                    ElevatedButton.icon(
                      onPressed: () =>
                          setState(() => _seriesFuture = _loadSeries()),
                      icon: const Icon(Icons.refresh),
                      label: const Text('Retry'),
                    ),
                  ],
                ),
              ),
            );
          }
          if (!snapshot.hasData) {
            return const Center(child: CircularProgressIndicator());
          }

          final series = snapshot.data!
              .map((d) => WeeklySeries.fromFirestore(d.data(), d.id))
              .whereType<WeeklySeries>();
          final days = buildWeeklyTimetable(
            series,
            nowSec: DateTime.now().millisecondsSinceEpoch ~/ 1000,
            latitude: widget.latitude,
            longitude: widget.longitude,
            radiusMiles: widget.radiusMiles,
          );

          if (days.isEmpty) {
            return Center(
              child: Text(
                filtered
                    ? 'No weekly programs within ${widget.radiusMiles.toStringAsFixed(0)} miles.'
                    : 'No weekly programs found.',
              ),
            );
          }

          return ListView(
            padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
            children: [
              if (!filtered)
                const Padding(
                  padding: EdgeInsets.only(bottom: 8),
                  child: Text('Enable location to see programs near you.'),
                ),
              for (final day in days) ...[
                _DayHeader(day: day),
                for (final entry in day.entries) _SeriesTile(entry: entry),
              ],
            ],
          );
        },
      ),
    );
  }
}

class _DayHeader extends StatelessWidget {
  final TimetableDay day;

  const _DayHeader({required this.day});

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.fromLTRB(4, 20, 4, 8),
      child: Text(
        '${day.name}  ·  ${day.entries.length}',
        style: theme.textTheme.titleMedium?.copyWith(
          fontWeight: FontWeight.bold,
          color: theme.colorScheme.primary,
        ),
      ),
    );
  }
}

class _SeriesTile extends StatelessWidget {
  final TimetableEntry entry;

  const _SeriesTile({required this.entry});

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final s = entry.series;
    // End time under the start, or the typical length when the source listed no end
    final end = s.durationEstimated && s.durationMinutes > 0
        ? '~${s.durationMinutes} min'
        : s.localEndTime;
    final details = [
      if (s.ageRange != null && s.ageRange!.isNotEmpty) s.ageRange!,
      s.interval == 2 ? 'Every other week' : 'Every week',
      if (entry.distanceMiles != null)
        '${entry.distanceMiles!.toStringAsFixed(1)} mi',
    ].join('  ·  ');

    return Card(
      margin: const EdgeInsets.only(bottom: 8),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            SizedBox(
              width: 64,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    s.localStartTime,
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  if (end != null)
                    Text(
                      end,
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: theme.colorScheme.onSurfaceVariant,
                      ),
                    ),
                ],
              ),
            ),
            const SizedBox(width: 8),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    s.title,
                    style: theme.textTheme.titleSmall?.copyWith(
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 2),
                  Text(displayVenue(s.venue), style: theme.textTheme.bodySmall),
                  const SizedBox(height: 4),
                  Text(
                    details,
                    style: theme.textTheme.bodySmall?.copyWith(
                      color: theme.colorScheme.onSurfaceVariant,
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
