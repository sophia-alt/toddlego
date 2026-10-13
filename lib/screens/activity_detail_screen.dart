import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

import '../filters.dart';
import '../models/activity.dart';
import '../theme.dart';
import '../widgets/components.dart';

class ActivityDetailScreen extends StatelessWidget {
  final Activity activity;
  final double? distanceMiles;

  const ActivityDetailScreen({
    super.key,
    required this.activity,
    this.distanceMiles,
  });

  Future<void> _open(BuildContext context, Uri uri) async {
    final ok = await launchUrl(uri, mode: LaunchMode.externalApplication);
    if (!ok && context.mounted) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('Could not open the link.')));
    }
  }

  @override
  Widget build(BuildContext context) {
    final a = activity;
    final ages = formatAge(a);
    final now = DateTime.now();
    final status = timeStatus(a, now);
    final trip = tripStatus(
      a,
      now,
      driveMinutes: distanceMiles == null ? null : driveMinutes(distanceMiles!),
    );
    final mapsUri = Uri.https('www.google.com', '/maps/search/', {
      'api': '1',
      'query': '${a.latitude},${a.longitude}',
    });
    final sourceUri = Uri.tryParse(a.sourceUrl);
    final hasSource =
        sourceUri != null && sourceUri.hasScheme && sourceUri.host.isNotEmpty;

    return Scaffold(
      appBar: AppBar(),
      body: ListView(
        padding: EdgeInsets.zero,
        children: [
          ActivityArt.banner(activity: a),
          Padding(
            padding: const EdgeInsets.fromLTRB(24, 28, 24, 32),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                if (status != null) ...[
                  TimeStatusTag(status: status),
                  const SizedBox(height: 10),
                ],
                Text(
                  a.title,
                  style: const TextStyle(
                    fontSize: 30,
                    fontWeight: FontWeight.w500,
                  ),
                ),
                const SizedBox(height: 8),
                Text(
                  a.venue,
                  style: const TextStyle(
                    fontSize: 18,
                    color: AppColors.textSecondary,
                  ),
                ),
                if (a.locationDetails?.isNotEmpty ?? false) ...[
                  const SizedBox(height: 4),
                  Text(
                    a.locationDetails!,
                    style: const TextStyle(
                      fontSize: 16,
                      color: AppColors.textSecondary,
                    ),
                  ),
                ],
                if (a.venueId != null)
                  _VenueAddress(
                    venueId: a.venueId!,
                    onTap: () => _open(context, mapsUri),
                  ),
                const SizedBox(height: 16),
                Text(
                  formatWhen(a, now),
                  style: const TextStyle(fontSize: 19),
                ),
                if (trip != null) ...[
                  const SizedBox(height: 12),
                  TripStatusBadge(status: trip),
                ],
                const SizedBox(height: 24),
                const Divider(color: AppColors.surfaceMuted, height: 1),
                const SizedBox(height: 16),
                if (formatDuration(a) != null)
                  _Row(
                    '⏱',
                    a.durationEstimated
                        ? 'About ${formatMinutes(a.durationMinutes)} · end time '
                              'not listed, check before you go'
                        : 'Lasts ${formatMinutes(a.durationMinutes)}',
                  ),
                _Row(
                  '👶',
                  ages == 'All ages'
                      ? 'Suitable for all ages'
                      : 'Suitable for ages $ages',
                ),
                _Row(
                  a.isIndoor ? '🏠' : '🌳',
                  a.isIndoor ? 'Indoor' : 'Outdoor',
                ),
                if (distanceMiles != null)
                  _Row('📍', '${driveMinutes(distanceMiles!)} min drive'),
                if (a.isFree) const _Row('💵', 'Free'),
                if (a.requiresBooking)
                  const _Row('📝', 'Registration required'),
                if (a.description != null &&
                    a.description!.trim().isNotEmpty) ...[
                  const SizedBox(height: 16),
                  Text(
                    a.description!.trim(),
                    style: const TextStyle(
                      fontSize: 17,
                      height: 1.6,
                      color: AppColors.textSecondary,
                    ),
                  ),
                ],
                const SizedBox(height: 24),
                const Divider(color: AppColors.surfaceMuted, height: 1),
                const SizedBox(height: 32),
                SizedBox(
                  width: double.infinity,
                  height: 56,
                  child: FilledButton(
                    style: FilledButton.styleFrom(
                      backgroundColor: AppColors.primary,
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(16),
                      ),
                    ),
                    onPressed: () => _open(context, mapsUri),
                    child: const Text(
                      'Open in Google Maps',
                      style: TextStyle(fontSize: 18),
                    ),
                  ),
                ),
                if (hasSource) ...[
                  const SizedBox(height: 14),
                  SizedBox(
                    width: double.infinity,
                    height: 56,
                    child: OutlinedButton(
                      style: OutlinedButton.styleFrom(
                        foregroundColor: AppColors.text,
                        side: const BorderSide(
                          color: AppColors.border,
                          width: 1.5,
                        ),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(16),
                        ),
                      ),
                      onPressed: () => _open(context, sourceUri),
                      child: const Text(
                        'View official event page',
                        style: TextStyle(fontSize: 18),
                      ),
                    ),
                  ),
                ],
              ],
            ),
          ),
        ],
      ),
    );
  }
}

/// Street address from the activity's `venues` doc; tapping it opens the map.
/// Shows nothing while loading or when the venue has no address.
class _VenueAddress extends StatefulWidget {
  final String venueId;
  final VoidCallback onTap;

  const _VenueAddress({required this.venueId, required this.onTap});

  @override
  State<_VenueAddress> createState() => _VenueAddressState();
}

class _VenueAddressState extends State<_VenueAddress> {
  // Fetched once, not on every rebuild
  late final Future<String?> _address = FirebaseFirestore.instance
      .collection('venues')
      .doc(widget.venueId)
      .get()
      .then((doc) => (doc.data()?['address'] as String?)?.trim());

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<String?>(
      future: _address,
      builder: (context, snapshot) {
        final address = snapshot.data;
        if (address == null || address.isEmpty) return const SizedBox.shrink();
        return Padding(
          padding: const EdgeInsets.only(top: 6),
          child: InkWell(
            onTap: widget.onTap,
            borderRadius: BorderRadius.circular(8),
            child: Text(
              address,
              style: const TextStyle(
                fontSize: 16,
                color: AppColors.primary,
                decoration: TextDecoration.underline,
                decorationColor: AppColors.primary,
              ),
            ),
          ),
        );
      },
    );
  }
}

class _Row extends StatelessWidget {
  final String emoji;
  final String text;

  const _Row(this.emoji, this.text);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 10),
      child: Row(
        children: [
          Text(emoji, style: const TextStyle(fontSize: 20)),
          const SizedBox(width: 14),
          Expanded(child: Text(text, style: const TextStyle(fontSize: 18))),
        ],
      ),
    );
  }
}
