import 'package:flutter/material.dart';

import '../filters.dart';
import '../models/activity.dart';
import '../theme.dart';
import 'components.dart';

/// Home list card: art, title, venue, when, drive time / age / indoor, booking badge.
class ActivityCard extends StatelessWidget {
  final Activity activity;
  final double? distanceMiles;
  final DateTime now;
  final VoidCallback onTap;

  const ActivityCard({
    super.key,
    required this.activity,
    required this.distanceMiles,
    required this.now,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    final a = activity;
    return Padding(
      padding: const EdgeInsets.only(bottom: 16),
      child: Material(
        color: Colors.white,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(22),
          side: const BorderSide(color: AppColors.border),
        ),
        child: InkWell(
          borderRadius: BorderRadius.circular(22),
          onTap: onTap,
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                ActivityArt(activity: a),
                const SizedBox(width: 16),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        a.title,
                        maxLines: 2,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          fontSize: 19,
                          fontWeight: FontWeight.w500,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        a.venue,
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          fontSize: 15,
                          color: AppColors.textSecondary,
                        ),
                      ),
                      const SizedBox(height: 8),
                      Text(
                        formatWhen(a, now),
                        style: const TextStyle(fontSize: 16),
                      ),
                      const SizedBox(height: 10),
                      Wrap(
                        spacing: 14,
                        runSpacing: 6,
                        children: [
                          if (distanceMiles != null)
                            _Info(
                              '📍',
                              '${driveMinutes(distanceMiles!)} min drive',
                            ),
                          _Info('👶', formatAge(a)),
                          Text(
                            a.isIndoor ? '🏠' : '🌳',
                            style: const TextStyle(fontSize: 15),
                          ),
                        ],
                      ),
                      const SizedBox(height: 8),
                      BookingBadge(requiresBooking: a.requiresBooking),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _Info extends StatelessWidget {
  final String emoji;
  final String text;

  const _Info(this.emoji, this.text);

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(emoji, style: const TextStyle(fontSize: 14)),
        const SizedBox(width: 4),
        Text(
          text,
          style: const TextStyle(fontSize: 15, color: AppColors.textSecondary),
        ),
      ],
    );
  }
}
