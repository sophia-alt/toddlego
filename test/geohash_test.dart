import 'package:flutter_test/flutter_test.dart';
import 'package:toddlego/geohash.dart';

/// Expected values produced by geofire-common 6.0.0 (functions/node_modules),
/// which the Cloud Functions use to write `geohash`.
const cases = [
  (
    37.5630,
    -122.3255,
    16093.44,
    '9q9j8qjk36',
    [
      ('9q9h', '9q9k'), ('9q8u', '9q8w'), ('9q9n', '9q9q'), ('9q8y', '9q8~'), //
    ],
  ),
  (
    37.4220,
    -122.0841,
    8046.72,
    '9q9hvumnfq',
    [
      ('9q9hh', '9q9h~'),
      ('9q9h0', '9q9hh'),
      ('9q9jh', '9q9j~'),
      ('9q9j0', '9q9jh'),
    ],
  ),
  (
    37.7749,
    -122.4194,
    40233.6,
    '9q8yyk8ytp',
    [('9q8h', '9q8~'), ('9q9h', '9q9~'), ('9qb0', '9qbh'), ('9qc0', '9qch')],
  ),
  (
    38.4404,
    -122.7141,
    1609.344,
    '9qbdxtf7d1',
    [('9qbdxh', '9qbdx~'), ('9qbdz0', '9qbdzh'), ('9qbdx0', '9qbdxh')],
  ),
  // Across the antimeridian
  (
    0.5,
    179.9,
    20000.0,
    'xbpfvep0ty',
    [('xbpd', 'xbph'), ('8004', '8008'), ('xbp8', 'xbpd'), ('8000', '8004')],
  ),
  // Southern and eastern hemispheres
  (
    -33.87,
    151.21,
    5000.0,
    'r3gx2f5ubq',
    [
      ('r3gx0', 'r3gx8'),
      ('r3grh', 'r3grs'),
      ('r3gx8', 'r3gxh'),
      ('r3grs', 'r3gr~'),
    ],
  ),
];

void main() {
  for (final (lat, lng, radius, hash, bounds) in cases) {
    test('matches geofire-common at ($lat, $lng), ${radius.round()} m', () {
      expect(geohashForLocation(lat, lng), hash);
      expect(geohashQueryBounds(lat, lng, radius), bounds);
    });
  }

  test('every point within the radius falls in one of the bounds', () {
    const lat = 37.5630, lng = -122.3255;
    final bounds = geohashQueryBounds(lat, lng, 16093.44); // 10 miles
    // Points ~9.5 miles away in eight directions
    for (final (dLat, dLng) in [
      (0.137, 0.0),
      (-0.137, 0.0),
      (0.0, 0.173),
      (0.0, -0.173),
      (0.097, 0.122),
      (0.097, -0.122),
      (-0.097, 0.122),
      (-0.097, -0.122),
    ]) {
      final h = geohashForLocation(lat + dLat, lng + dLng);
      expect(
        bounds.any((b) => h.compareTo(b.$1) >= 0 && h.compareTo(b.$2) <= 0),
        isTrue,
        reason: '$h at ($dLat, $dLng) not covered by $bounds',
      );
    }
  });
}
