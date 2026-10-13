import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:toddlego/feedback.dart';

void main() {
  test('debug log keeps the 30 newest entries, oldest first', () {
    DebugLog.clear();
    for (var i = 0; i < 35; i++) {
      DebugLog.add('error $i');
    }
    expect(DebugLog.recent, hasLength(30));
    expect(DebugLog.recent.first, endsWith('error 5'));
    expect(DebugLog.recent.last, endsWith('error 34'));
  });

  test('debug log shortens long messages', () {
    DebugLog.clear();
    DebugLog.add('x' * 1000);
    expect(DebugLog.recent.single.length, lessThan(320));
  });

  test('category keys match what firestore.rules accepts', () {
    final rules = File('firestore.rules').readAsStringSync();
    final allowed = RegExp(
      r"category in \[([^\]]*)\]",
    ).firstMatch(rules)!.group(1)!;
    final keys = RegExp(r"'([a-z_]+)'").allMatches(allowed).map((m) => m[1]);
    expect(keys, FeedbackCategory.values.map((c) => c.key));
  });
}
