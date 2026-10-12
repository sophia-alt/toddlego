import 'dart:math' as math;

/// Geohash encoding and radius query bounds, ported from geofire-common 6.0.0
/// (the library the Cloud Functions use to write `geohash`), so the app's
/// queries line up exactly with the stored values.

const String _base32 = '0123456789bcdefghjkmnpqrstuvwxyz';
const int _geohashPrecision = 10;
const int _bitsPerChar = 5;
const int _maximumBitsPrecision = 22 * _bitsPerChar;
const double _earthMeriCircumference = 40007860;
const double _metersPerDegreeLatitude = 110574;
const double _earthEqRadius = 6378137.0;
const double _e2 = 0.00669447819799;
const double _epsilon = 1e-12;

double _log2(double x) => math.log(x) / math.log(2);

double _degreesToRadians(double degrees) => degrees * math.pi / 180;

/// Geohash of a point; 10 characters by default, like geohashForLocation.
String geohashForLocation(
  double latitude,
  double longitude, [
  int precision = _geohashPrecision,
]) {
  var latMin = -90.0, latMax = 90.0, lngMin = -180.0, lngMax = 180.0;
  final hash = StringBuffer();
  var hashVal = 0;
  var bits = 0;
  var even = true;
  while (hash.length < precision) {
    final val = even ? longitude : latitude;
    final mid = even ? (lngMin + lngMax) / 2 : (latMin + latMax) / 2;
    if (val > mid) {
      hashVal = (hashVal << 1) + 1;
      if (even) {
        lngMin = mid;
      } else {
        latMin = mid;
      }
    } else {
      hashVal = hashVal << 1;
      if (even) {
        lngMax = mid;
      } else {
        latMax = mid;
      }
    }
    even = !even;
    if (bits < 4) {
      bits++;
    } else {
      bits = 0;
      hash.write(_base32[hashVal]);
      hashVal = 0;
    }
  }
  return hash.toString();
}

double _metersToLongitudeDegrees(double distance, double latitude) {
  final radians = _degreesToRadians(latitude);
  final num = math.cos(radians) * _earthEqRadius * math.pi / 180;
  final denom = 1 / math.sqrt(1 - _e2 * math.sin(radians) * math.sin(radians));
  final deltaDeg = num * denom;
  if (deltaDeg < _epsilon) return distance > 0 ? 360 : 0;
  return math.min(360, distance / deltaDeg);
}

double _longitudeBitsForResolution(double resolution, double latitude) {
  final degs = _metersToLongitudeDegrees(resolution, latitude);
  return degs.abs() > 0.000001 ? math.max(1, _log2(360 / degs)) : 1;
}

double _latitudeBitsForResolution(double resolution) => math.min(
  _log2(_earthMeriCircumference / 2 / resolution),
  _maximumBitsPrecision.toDouble(),
);

double _wrapLongitude(double longitude) {
  if (longitude <= 180 && longitude >= -180) return longitude;
  final adjusted = longitude + 180;
  // JS `%` keeps the dividend's sign, like Dart's remainder()
  if (adjusted > 0) return adjusted.remainder(360) - 180;
  return 180 - (-adjusted).remainder(360);
}

int _boundingBoxBits(double latitude, double size) {
  final latDeltaDegrees = size / _metersPerDegreeLatitude;
  final latitudeNorth = math.min(90.0, latitude + latDeltaDegrees);
  final latitudeSouth = math.max(-90.0, latitude - latDeltaDegrees);
  final bitsLat = _latitudeBitsForResolution(size).floor() * 2;
  final bitsLongNorth =
      _longitudeBitsForResolution(size, latitudeNorth).floor() * 2 - 1;
  final bitsLongSouth =
      _longitudeBitsForResolution(size, latitudeSouth).floor() * 2 - 1;
  return [
    bitsLat,
    bitsLongNorth,
    bitsLongSouth,
    _maximumBitsPrecision,
  ].reduce(math.min);
}

List<(double, double)> _boundingBoxCoordinates(
  double latitude,
  double longitude,
  double radius,
) {
  final latDegrees = radius / _metersPerDegreeLatitude;
  final latitudeNorth = math.min(90.0, latitude + latDegrees);
  final latitudeSouth = math.max(-90.0, latitude - latDegrees);
  final longDegs = math.max(
    _metersToLongitudeDegrees(radius, latitudeNorth),
    _metersToLongitudeDegrees(radius, latitudeSouth),
  );
  final west = _wrapLongitude(longitude - longDegs);
  final east = _wrapLongitude(longitude + longDegs);
  return [
    (latitude, longitude),
    (latitude, west),
    (latitude, east),
    (latitudeNorth, longitude),
    (latitudeNorth, west),
    (latitudeNorth, east),
    (latitudeSouth, longitude),
    (latitudeSouth, west),
    (latitudeSouth, east),
  ];
}

(String, String) _geohashQuery(String geohash, int bits) {
  final precision = (bits / _bitsPerChar).ceil();
  if (geohash.length < precision) return (geohash, '$geohash~');
  final hash = geohash.substring(0, precision);
  final base = hash.substring(0, hash.length - 1);
  final lastValue = _base32.indexOf(hash[hash.length - 1]);
  final significantBits = bits - base.length * _bitsPerChar;
  final unusedBits = _bitsPerChar - significantBits;
  final startValue = (lastValue >> unusedBits) << unusedBits;
  final endValue = startValue + (1 << unusedBits);
  if (endValue > 31) return ('$base${_base32[startValue]}', '$base~');
  return ('$base${_base32[startValue]}', '$base${_base32[endValue]}');
}

/// [start, end] geohash ranges that together cover a circle of [radiusMeters].
/// Query each with `geohash >= start` and `geohash <= end`, then drop results
/// outside the radius (the ranges cover a slightly larger area).
List<(String, String)> geohashQueryBounds(
  double latitude,
  double longitude,
  double radiusMeters,
) {
  final queryBits = math.max(1, _boundingBoxBits(latitude, radiusMeters));
  final precision = (queryBits / _bitsPerChar).ceil();
  final bounds = <(String, String)>[];
  for (final (lat, lng) in _boundingBoxCoordinates(
    latitude,
    longitude,
    radiusMeters,
  )) {
    final q = _geohashQuery(geohashForLocation(lat, lng, precision), queryBits);
    if (!bounds.contains(q)) bounds.add(q);
  }
  return bounds;
}
