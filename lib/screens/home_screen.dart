import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:geolocator/geolocator.dart';

import '../cities.dart';
import '../filters.dart';
import '../models/activity.dart';
import '../settings_store.dart';
import '../theme.dart';
import '../weekly_schedule.dart' show distanceMiles;
import '../weekly_schedule_screen.dart';
import '../widgets/activity_card.dart';
import '../widgets/components.dart';
import 'activity_detail_screen.dart';
import 'settings_screen.dart';

/// Activities within this distance of the user (or the picked city) are listed.
const double _radiusMiles = 10;

/// How far ahead activities are loaded: covers "This Week". Every matching doc is
/// downloaded on each open, and library feeds list thousands of upcoming events.
const int _daysAhead = 7;

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> with WidgetsBindingObserver {
  AppSettings _settings = const AppSettings();
  bool _filtersExpanded = true;

  Position? _gpsPosition;
  bool _isLocating = false;
  String _locationError = '';

  // Created once so rebuilds (filter or location changes) don't re-subscribe.
  late Stream<QuerySnapshot> _activitiesStream;
  StreamSubscription<ServiceStatus>? _serviceStatusSub;

  // Without a limit, getCurrentPosition waits forever when the device has no fix.
  static const Duration _locationTimeout = Duration(seconds: 15);

  BayAreaCity? get _pickedCity => cityNamed(_settings.cityName);

  /// The point distances are measured from: the picked city, else the device.
  ({double lat, double lng})? get _origin {
    final city = _pickedCity;
    if (city != null) return (lat: city.latitude, lng: city.longitude);
    final p = _gpsPosition;
    return p == null ? null : (lat: p.latitude, lng: p.longitude);
  }

  /// "San Mateo, CA" under "Near you".
  String get _placeLabel {
    final city = _pickedCity;
    if (city != null) return city.label;
    final p = _gpsPosition;
    if (p != null) {
      return nearestCity(p.latitude, p.longitude)?.label ?? 'Your location';
    }
    if (_isLocating) return 'Finding your location…';
    return 'Location off';
  }

  @override
  void initState() {
    super.initState();
    _activitiesStream = _upcomingActivities();
    WidgetsBinding.instance.addObserver(this);

    // Retry as soon as the user turns location services on.
    if (!kIsWeb) {
      _serviceStatusSub = Geolocator.getServiceStatusStream().listen((status) {
        if (status == ServiceStatus.enabled &&
            _gpsPosition == null &&
            _pickedCity == null) {
          _getCurrentLocation();
        }
      });
    }

    SettingsStore.load().then((settings) {
      if (!mounted) return;
      setState(() => _settings = settings);
      if (_pickedCity == null) _getCurrentLocation();
    });
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _serviceStatusSub?.cancel();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    // The user may have granted permission or enabled location in system settings.
    if (state == AppLifecycleState.resumed &&
        _gpsPosition == null &&
        _pickedCity == null) {
      _getCurrentLocation();
    }
  }

  Future<void> _getCurrentLocation() async {
    if (_isLocating) return;
    setState(() {
      _isLocating = true;
    });

    try {
      if (!await Geolocator.isLocationServiceEnabled()) {
        _setLocationError('Location services are disabled.');
        return;
      }

      LocationPermission permission = await Geolocator.checkPermission();
      if (permission == LocationPermission.denied) {
        permission = await Geolocator.requestPermission();
        if (permission == LocationPermission.denied) {
          _setLocationError('Location permissions are denied.');
          return;
        }
      }
      if (permission == LocationPermission.deniedForever) {
        _setLocationError('Location permissions are permanently denied.');
        return;
      }

      // Show distances right away from the last known fix, then refine.
      // (getLastKnownPosition is not supported on web.)
      // Every platform call is bounded: on Android these go through Google Play
      // services, which can hang without ever completing or failing.
      if (!kIsWeb && _gpsPosition == null) {
        try {
          final lastKnown = await Geolocator.getLastKnownPosition().timeout(
            const Duration(seconds: 3),
          );
          if (lastKnown != null) _setPosition(lastKnown);
        } on TimeoutException {
          // Fall through to a fresh fix below.
        }
      }

      try {
        final position = await Geolocator.getCurrentPosition(
          locationSettings: const LocationSettings(
            accuracy: LocationAccuracy.high,
            timeLimit: _locationTimeout,
          ),
        ).timeout(_locationTimeout + const Duration(seconds: 5));
        _setPosition(position);
      } on TimeoutException {
        // Keep the last known position if we have one; otherwise offer a retry.
        if (_gpsPosition == null) {
          _setLocationError('Could not get your location. Tap to retry.');
        }
      }
    } catch (e) {
      _setLocationError('Error getting location: $e');
    } finally {
      if (mounted) {
        setState(() {
          _isLocating = false;
        });
      }
    }
  }

  void _setPosition(Position position) {
    if (!mounted) return;
    setState(() {
      _gpsPosition = position;
      _locationError = '';
    });
  }

  void _setLocationError(String message) {
    if (!mounted) return;
    setState(() {
      _locationError = message;
    });
  }

  void _applySettings(AppSettings next) {
    final wasCity = _pickedCity;
    setState(() => _settings = next);
    SettingsStore.save(next);
    // Switched back to "Use my location": find the device position
    if (wasCity != null && _pickedCity == null && _gpsPosition == null) {
      _getCurrentLocation();
    }
  }

  void _openSettings() {
    final p = _gpsPosition;
    Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => SettingsScreen(
          settings: _settings,
          onChanged: _applySettings,
          deviceLocationLabel: p == null
              ? null
              : nearestCity(p.latitude, p.longitude)?.label,
        ),
      ),
    );
  }

  void _openWeekly() {
    final origin = _origin;
    Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => WeeklyScheduleScreen(
          latitude: origin?.lat,
          longitude: origin?.lng,
          radiusMiles: _radiusMiles,
        ),
      ),
    );
  }

  double? _distanceTo(Activity a) {
    final origin = _origin;
    if (origin == null || (a.latitude == 0 && a.longitude == 0)) return null;
    return distanceMiles(origin.lat, origin.lng, a.latitude, a.longitude);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: StreamBuilder<QuerySnapshot>(
          stream: _activitiesStream,
          builder: (context, snapshot) {
            return CustomScrollView(
              slivers: [
                SliverPadding(
                  padding: const EdgeInsets.fromLTRB(20, 24, 20, 0),
                  sliver: SliverList.list(
                    children: [
                      _header(),
                      const SizedBox(height: 20),
                      _filterBar(),
                      if (_filtersExpanded) ...[
                        const SizedBox(height: 20),
                        _filterPanel(),
                      ],
                      const SizedBox(height: 24),
                    ],
                  ),
                ),
                ..._results(snapshot),
              ],
            );
          },
        ),
      ),
    );
  }

  Widget _header() {
    final retry =
        _pickedCity == null &&
        _gpsPosition == null &&
        !_isLocating &&
        _locationError.isNotEmpty;
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const Text(
                'Near you',
                style: TextStyle(fontSize: 26, fontWeight: FontWeight.w500),
              ),
              const SizedBox(height: 4),
              GestureDetector(
                onTap: retry ? _getCurrentLocation : null,
                child: Text(
                  retry ? '$_placeLabel · tap to retry' : _placeLabel,
                  style: const TextStyle(
                    fontSize: 19,
                    color: AppColors.textMuted,
                  ),
                ),
              ),
            ],
          ),
        ),
        IconButton(
          icon: const Icon(Icons.calendar_view_week_outlined, size: 28),
          tooltip: 'Every week',
          onPressed: _openWeekly,
        ),
        IconButton(
          icon: const Icon(Icons.settings_outlined, size: 30),
          tooltip: 'Settings',
          onPressed: _openSettings,
        ),
      ],
    );
  }

  Widget _filterBar() {
    return Material(
      color: AppColors.surfaceMuted,
      borderRadius: BorderRadius.circular(20),
      child: InkWell(
        borderRadius: BorderRadius.circular(20),
        onTap: () => setState(() => _filtersExpanded = !_filtersExpanded),
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 14),
          child: Row(
            children: [
              const Text('Filters', style: TextStyle(fontSize: 18)),
              const SizedBox(width: 12),
              Expanded(
                child: Wrap(
                  spacing: 8,
                  runSpacing: 6,
                  children: [
                    for (final c in _settings.filters.summaryChips)
                      SummaryChip(c),
                  ],
                ),
              ),
              Icon(
                _filtersExpanded
                    ? Icons.keyboard_arrow_up
                    : Icons.keyboard_arrow_down,
                color: AppColors.textSecondary,
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _filterPanel() {
    final f = _settings.filters;
    void set(ActivityFilters next) =>
        _applySettings(_settings.copyWith(filters: next));
    return Column(
      children: [
        SolidChoice<TimeFrame>(
          options: const [
            (TimeFrame.today, 'Today'),
            (TimeFrame.thisWeek, 'This Week'),
          ],
          selected: f.timeFrame,
          onSelected: (v) => set(f.copyWith(timeFrame: v)),
        ),
        const SizedBox(height: 16),
        OutlineChoice<AgeBand>(
          options: [for (final b in AgeBand.values) (b, b.label)],
          selected: f.ageBand,
          onSelected: (v) => set(f.copyWith(ageBand: () => v)),
        ),
        const SizedBox(height: 16),
        OutlineToggle(
          label: f.indoorOnly ? '🏠 Indoor only' : '🌳 Indoor & Outdoor',
          selected: f.indoorOnly,
          onTap: () => set(f.copyWith(indoorOnly: !f.indoorOnly)),
        ),
      ],
    );
  }

  List<Widget> _results(AsyncSnapshot<QuerySnapshot> snapshot) {
    Widget message(String text, {Widget? action}) => SliverFillRemaining(
      hasScrollBody: false,
      child: Padding(
        padding: const EdgeInsets.all(32),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Text(
              text,
              textAlign: TextAlign.center,
              style: const TextStyle(
                fontSize: 16,
                color: AppColors.textSecondary,
              ),
            ),
            if (action != null) ...[const SizedBox(height: 16), action],
          ],
        ),
      ),
    );

    if (snapshot.hasError) {
      return [
        message(
          'Could not load activities.\n${snapshot.error}',
          action: OutlinedButton.icon(
            onPressed: () =>
                setState(() => _activitiesStream = _upcomingActivities()),
            icon: const Icon(Icons.refresh),
            label: const Text('Retry'),
          ),
        ),
      ];
    }
    if (!snapshot.hasData) {
      return [
        const SliverFillRemaining(
          hasScrollBody: false,
          child: Center(child: CircularProgressIndicator()),
        ),
      ];
    }

    final now = DateTime.now();
    final origin = _origin;
    final items = <({Activity activity, double? distance})>[];
    for (final doc in snapshot.data!.docs) {
      final Activity a;
      try {
        a = Activity.fromFirestore(doc.data() as Map<String, dynamic>, doc.id);
      } catch (e) {
        debugPrint('Failed to parse activity ${doc.id}: $e');
        continue;
      }
      if (!_settings.filters.matches(a, now)) continue;
      final d = _distanceTo(a);
      if (origin != null && (d == null || d > _radiusMiles)) continue;
      items.add((activity: a, distance: d));
    }
    // Nearest first; without a location, soonest first
    items.sort(
      (x, y) => origin != null
          ? x.distance!.compareTo(y.distance!)
          : x.activity.startTime.compareTo(y.activity.startTime),
    );

    if (items.isEmpty) {
      final when = _settings.filters.timeFrame == TimeFrame.today
          ? 'today'
          : 'this week';
      return [
        message(
          origin == null
              ? 'No activities found $when.\nTurn on location or pick a city in Settings to see what is near you.'
              : 'No activities within ${_radiusMiles.toStringAsFixed(0)} miles $when.\nTry another filter.',
        ),
      ];
    }

    return [
      SliverPadding(
        padding: const EdgeInsets.fromLTRB(20, 0, 20, 24),
        sliver: SliverList.builder(
          itemCount: items.length,
          itemBuilder: (context, i) {
            final item = items[i];
            return ActivityCard(
              activity: item.activity,
              distanceMiles: item.distance,
              now: now,
              onTap: () => Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => ActivityDetailScreen(
                    activity: item.activity,
                    distanceMiles: item.distance,
                  ),
                ),
              ),
            );
          },
        ),
      ),
    ];
  }

  Stream<QuerySnapshot> _upcomingActivities() {
    final now = DateTime.now();
    final todayStart = DateTime(now.year, now.month, now.day);
    final todayStartSec = todayStart.millisecondsSinceEpoch ~/ 1000;
    final windowEndSec =
        todayStart
            .add(const Duration(days: _daysAhead))
            .millisecondsSinceEpoch ~/
        1000;

    return FirebaseFirestore.instance
        .collection('activities')
        .where('startTime', isGreaterThanOrEqualTo: todayStartSec)
        .where('startTime', isLessThan: windowEndSec)
        .orderBy('startTime', descending: false)
        .snapshots();
  }
}
