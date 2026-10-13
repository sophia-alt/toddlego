import 'package:flutter_test/flutter_test.dart';
import 'package:toddlego/models/activity.dart';

Map<String, dynamic> doc(Map<String, dynamic> extra) => {
  'title': 'Puppet Show',
  'venue': "Children's Fairyland",
  'startTime': 1792000800,
  'endTime': 1792002600,
  ...extra,
};

void main() {
  test('reads an admission note as the cost', () {
    final a = Activity.fromFirestore(
      doc({'isFree': false, 'cost': 'Included with park admission'}),
      'tec_fairyland_1',
    );
    expect(a.isFree, isFalse);
    expect(a.cost, 'Included with park admission');
  });

  test('no cost field (library programs) or a blank one means unknown', () {
    expect(Activity.fromFirestore(doc({}), 'a').cost, isNull);
    expect(Activity.fromFirestore(doc({'cost': '  '}), 'b').cost, isNull);
    expect(Activity.fromFirestore(doc({}), 'a').isFree, isTrue);
  });
}
