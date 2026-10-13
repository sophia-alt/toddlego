import 'dart:async';
import 'dart:io' show Platform;

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:package_info_plus/package_info_plus.dart';

/// What a feedback report is about. [key] must match the list in
/// firestore.rules and CATEGORY_TAGS in functions/utils/feedbackEmail.js.
enum FeedbackCategory {
  bug('bug', '🐞 Bug', 'What happened, and what did you expect?'),
  eventInfo(
    'event_info',
    '📍 Wrong info',
    'Which detail is wrong: time, place, age, cancelled?',
  ),
  feature('feature', '💡 Idea', 'What would make ToddleGo more useful?'),
  other('other', '💬 Other', 'Tell us anything.');

  const FeedbackCategory(this.key, this.label, this.hint);

  final String key;
  final String label;

  /// Placeholder in the message box.
  final String hint;
}

/// The last few errors and debug messages, attached to feedback so a report
/// like "the list never loads" arrives with the error behind it.
class DebugLog {
  static const _maxEntries = 30;
  static const _maxEntryChars = 300;
  static final _entries = <String>[];

  /// Oldest first.
  static List<String> get recent => List.unmodifiable(_entries);

  static void add(String message) {
    final t = DateTime.now();
    String two(int n) => n.toString().padLeft(2, '0');
    final text = message.length > _maxEntryChars
        ? '${message.substring(0, _maxEntryChars)}…'
        : message;
    _entries.add('${two(t.hour)}:${two(t.minute)}:${two(t.second)} $text');
    if (_entries.length > _maxEntries) _entries.removeAt(0);
  }

  @visibleForTesting
  static void clear() => _entries.clear();

  /// Records Flutter errors, uncaught async errors and debugPrint output,
  /// keeping the default handling of each.
  static void install() {
    final print = debugPrint;
    debugPrint = (String? message, {int? wrapWidth}) {
      if (message != null) add(message);
      print(message, wrapWidth: wrapWidth);
    };
    final onFlutterError = FlutterError.onError;
    FlutterError.onError = (details) {
      add('FlutterError: ${details.exceptionAsString()}');
      onFlutterError?.call(details);
    };
    PlatformDispatcher.instance.onError = (error, stack) {
      add('Uncaught: $error');
      return false; // still reported as usual
    };
  }
}

/// Device and app details sent with every report, plus [screen]: what the
/// user was looking at (filters, location source, the activity, ...).
/// Shown to the user before sending.
Future<Map<String, Object?>> collectFeedbackContext(
  BuildContext context,
  Map<String, Object?> screen,
) async {
  final media = MediaQuery.of(context);
  final info = await PackageInfo.fromPlatform();
  final now = DateTime.now();
  final offset = now.timeZoneOffset;
  final sign = offset.isNegative ? '-' : '+';
  final hours = offset.inHours.abs().toString().padLeft(2, '0');
  final minutes = (offset.inMinutes.abs() % 60).toString().padLeft(2, '0');
  return {
    'appVersion': '${info.version}+${info.buildNumber}',
    'platform': kIsWeb ? 'web' : defaultTargetPlatform.name,
    if (!kIsWeb) 'os': Platform.operatingSystemVersion,
    'locale': PlatformDispatcher.instance.locale.toLanguageTag(),
    'screenSize':
        '${media.size.width.round()}x${media.size.height.round()} '
        '@${media.devicePixelRatio.toStringAsFixed(1)}x',
    'textScale': media.textScaler.scale(1),
    'localTime': now.toIso8601String(),
    'timeZone': '${now.timeZoneName} (UTC$sign$hours:$minutes)',
    ...screen,
    'recentLog': DebugLog.recent,
  };
}

/// Saves the report to `feedback`; the feedbackEmailer Cloud Function emails it.
/// Returns false when the device seems offline: Firestore keeps the report and
/// sends it once the connection is back.
Future<bool> submitFeedback({
  required FeedbackCategory category,
  required String message,
  String? email,
  required Map<String, Object?> context,
}) async {
  final write = FirebaseFirestore.instance.collection('feedback').add({
    'category': category.key,
    'message': message,
    if (email != null && email.isNotEmpty) 'email': email,
    'context': context,
    'createdAt': FieldValue.serverTimestamp(),
  });
  try {
    await write.timeout(const Duration(seconds: 15));
    return true;
  } on TimeoutException {
    return false;
  }
}
