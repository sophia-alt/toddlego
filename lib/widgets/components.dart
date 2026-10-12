import 'package:flutter/material.dart';

import '../filters.dart';
import '../models/activity.dart';
import '../theme.dart';

/// Placeholder art for an activity (no photos yet): category emoji on a tinted tile.
class ActivityArt extends StatelessWidget {
  final Activity activity;
  final double size;

  /// Square rounded tile of [size] for cards.
  const ActivityArt({super.key, required this.activity, this.size = 96});

  /// Full-width banner for the detail page.
  const ActivityArt.banner({super.key, required this.activity}) : size = 0;

  @override
  Widget build(BuildContext context) {
    final kind = activityKind(activity);
    final banner = size == 0;
    final tile = Container(
      width: banner ? double.infinity : size,
      height: banner ? 200 : size,
      decoration: BoxDecoration(
        color: Color(kind.background),
        borderRadius: banner ? null : BorderRadius.circular(18),
      ),
      alignment: Alignment.center,
      child: Text(
        kind.emoji,
        style: TextStyle(fontSize: banner ? 72 : size * 0.42),
      ),
    );
    return Semantics(label: '${kind.name} activity', child: tile);
  }
}

/// "Walk in" (green) or "Book ahead" (amber) pill.
class BookingBadge extends StatelessWidget {
  final bool requiresBooking;

  const BookingBadge({super.key, required this.requiresBooking});

  @override
  Widget build(BuildContext context) {
    final (bg, border, fg, icon, label) = requiresBooking
        ? (
            AppColors.bookBackground,
            AppColors.bookBorder,
            AppColors.bookText,
            Icons.event_available_outlined,
            'Book ahead',
          )
        : (
            AppColors.walkInBackground,
            AppColors.walkInBorder,
            AppColors.walkInText,
            Icons.check_circle_outline,
            'Walk in',
          );
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 5),
      decoration: BoxDecoration(
        color: bg,
        border: Border.all(color: border),
        borderRadius: BorderRadius.circular(20),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 16, color: fg),
          const SizedBox(width: 6),
          Text(
            label,
            style: TextStyle(
              color: fg,
              fontSize: 14,
              fontWeight: FontWeight.w500,
            ),
          ),
        ],
      ),
    );
  }
}

/// Solid tag above an activity's title: blue "Upcoming · in 25 min", green
/// "Ongoing · 15 min left" (with a dot), grey "Ended". Solid so it reads apart
/// from the outlined booking and trip badges.
class TimeStatusTag extends StatelessWidget {
  final TimeStatus status;

  const TimeStatusTag({super.key, required this.status});

  @override
  Widget build(BuildContext context) {
    final (bg, fg) = switch (status.state) {
      TimeState.upcoming => (AppColors.primary, Colors.white),
      TimeState.ongoing => (AppColors.ongoing, Colors.white),
      TimeState.ended => (AppColors.surfaceMuted, AppColors.textSecondary),
    };
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
      decoration: BoxDecoration(
        color: bg,
        borderRadius: BorderRadius.circular(6),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          if (status.state == TimeState.ongoing) ...[
            Container(
              width: 6,
              height: 6,
              decoration: BoxDecoration(color: fg, shape: BoxShape.circle),
            ),
            const SizedBox(width: 5),
          ],
          Flexible(
            child: Text(
              status.label,
              style: TextStyle(
                color: fg,
                fontSize: 12,
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

/// "You'd arrive ~5 min after it starts" (blue) or "Over before you can get there" (red).
class TripStatusBadge extends StatelessWidget {
  final TripStatus status;

  const TripStatusBadge({super.key, required this.status});

  @override
  Widget build(BuildContext context) {
    final (bg, border, fg, icon) = status.warn
        ? (
            AppColors.warnBackground,
            AppColors.warnBorder,
            AppColors.warnText,
            Icons.warning_amber_rounded,
          )
        : (
            AppColors.selectedBackground,
            AppColors.selectedBorder,
            AppColors.selectedText,
            Icons.schedule,
          );
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 5),
      decoration: BoxDecoration(
        color: bg,
        border: Border.all(color: border),
        borderRadius: BorderRadius.circular(20),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 16, color: fg),
          const SizedBox(width: 6),
          Flexible(
            child: Text(
              status.text,
              style: TextStyle(
                color: fg,
                fontSize: 14,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

/// Side-by-side buttons; the selected one is solid blue (Today / Tomorrow / This Week).
class SolidChoice<T> extends StatelessWidget {
  final List<(T, String)> options;
  final T? selected;
  final ValueChanged<T> onSelected;
  final double height;

  const SolidChoice({
    super.key,
    required this.options,
    required this.selected,
    required this.onSelected,
    this.height = 52,
  });

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        for (final (i, (value, label)) in options.indexed) ...[
          if (i > 0) const SizedBox(width: 12),
          Expanded(
            child: _Choice(
              label: label,
              selected: value == selected,
              height: height,
              background: value == selected
                  ? AppColors.primary
                  : AppColors.surfaceMuted,
              borderColor: Colors.transparent,
              foreground: value == selected
                  ? Colors.white
                  : AppColors.textSecondary,
              radius: 16,
              onTap: () => onSelected(value),
            ),
          ),
        ],
      ],
    );
  }
}

/// Outlined buttons; the selected one is light blue (age bands). Tapping the
/// selected one clears it when [allowNone] is set.
class OutlineChoice<T> extends StatelessWidget {
  final List<(T, String)> options;
  final T? selected;
  final ValueChanged<T?> onSelected;
  final bool allowNone;

  const OutlineChoice({
    super.key,
    required this.options,
    required this.selected,
    required this.onSelected,
    this.allowNone = true,
  });

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        for (final (i, (value, label)) in options.indexed) ...[
          if (i > 0) const SizedBox(width: 12),
          Expanded(
            child: OutlineToggle(
              label: label,
              selected: value == selected,
              onTap: () =>
                  onSelected(value == selected && allowNone ? null : value),
            ),
          ),
        ],
      ],
    );
  }
}

/// One outlined toggle button (also the "Indoor only" filter).
class OutlineToggle extends StatelessWidget {
  final String label;
  final bool selected;
  final VoidCallback onTap;

  const OutlineToggle({
    super.key,
    required this.label,
    required this.selected,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return _Choice(
      label: label,
      selected: selected,
      height: 44,
      background: selected ? AppColors.selectedBackground : Colors.white,
      borderColor: selected ? AppColors.selectedBorder : AppColors.border,
      foreground: selected ? AppColors.selectedText : AppColors.textSecondary,
      radius: 12,
      onTap: onTap,
    );
  }
}

class _Choice extends StatelessWidget {
  final String label;
  final bool selected;
  final double height;
  final Color background;
  final Color borderColor;
  final Color foreground;
  final double radius;
  final VoidCallback onTap;

  const _Choice({
    required this.label,
    required this.selected,
    required this.height,
    required this.background,
    required this.borderColor,
    required this.foreground,
    required this.radius,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return Semantics(
      button: true,
      selected: selected,
      child: Material(
        color: background,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(radius),
          side: BorderSide(color: borderColor),
        ),
        child: InkWell(
          borderRadius: BorderRadius.circular(radius),
          onTap: onTap,
          child: SizedBox(
            height: height,
            child: Center(
              child: Text(
                label,
                style: TextStyle(fontSize: 16, color: foreground),
              ),
            ),
          ),
        ),
      ),
    );
  }
}

/// Small grey summary chip in the collapsed filter bar.
class SummaryChip extends StatelessWidget {
  final String label;

  const SummaryChip(this.label, {super.key});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 5),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Text(
        label,
        style: const TextStyle(fontSize: 14, color: AppColors.textSecondary),
      ),
    );
  }
}
