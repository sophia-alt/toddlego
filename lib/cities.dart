import 'weekly_schedule.dart' show distanceMiles;

/// A Bay Area city the user can pick in Settings → Change city.
class BayAreaCity {
  final String name;
  final double latitude;
  final double longitude;

  const BayAreaCity(this.name, this.latitude, this.longitude);

  String get label => '$name, CA';
}

/// Approximate city-center coordinates, alphabetical.
const List<BayAreaCity> bayAreaCities = [
  BayAreaCity('Alameda', 37.7652, -122.2416),
  BayAreaCity('Antioch', 38.0049, -121.8058),
  BayAreaCity('Berkeley', 37.8715, -122.2730),
  BayAreaCity('Burlingame', 37.5841, -122.3661),
  BayAreaCity('Campbell', 37.2872, -121.9500),
  BayAreaCity('Castro Valley', 37.6941, -122.0864),
  BayAreaCity('Concord', 37.9780, -122.0311),
  BayAreaCity('Cupertino', 37.3230, -122.0322),
  BayAreaCity('Daly City', 37.6879, -122.4702),
  BayAreaCity('Dublin', 37.7022, -121.9358),
  BayAreaCity('Fremont', 37.5485, -121.9886),
  BayAreaCity('Gilroy', 37.0058, -121.5683),
  BayAreaCity('Half Moon Bay', 37.4636, -122.4286),
  BayAreaCity('Hayward', 37.6688, -122.0808),
  BayAreaCity('Livermore', 37.6819, -121.7680),
  BayAreaCity('Los Altos', 37.3852, -122.1141),
  BayAreaCity('Los Gatos', 37.2358, -121.9624),
  BayAreaCity('Menlo Park', 37.4530, -122.1817),
  BayAreaCity('Milpitas', 37.4323, -121.8996),
  BayAreaCity('Morgan Hill', 37.1305, -121.6544),
  BayAreaCity('Mountain View', 37.3861, -122.0839),
  BayAreaCity('Napa', 38.2975, -122.2869),
  BayAreaCity('Newark', 37.5297, -122.0402),
  BayAreaCity('Novato', 38.1074, -122.5697),
  BayAreaCity('Oakland', 37.8044, -122.2712),
  BayAreaCity('Palo Alto', 37.4419, -122.1430),
  BayAreaCity('Pleasanton', 37.6624, -121.8747),
  BayAreaCity('Redwood City', 37.4852, -122.2364),
  BayAreaCity('Richmond', 37.9358, -122.3477),
  BayAreaCity('San Francisco', 37.7749, -122.4194),
  BayAreaCity('San Jose', 37.3382, -121.8863),
  BayAreaCity('San Leandro', 37.7249, -122.1561),
  BayAreaCity('San Mateo', 37.5630, -122.3255),
  BayAreaCity('San Rafael', 37.9735, -122.5311),
  BayAreaCity('Santa Clara', 37.3541, -121.9552),
  BayAreaCity('Santa Rosa', 38.4404, -122.7141),
  BayAreaCity('Saratoga', 37.2638, -122.0230),
  BayAreaCity('South San Francisco', 37.6547, -122.4077),
  BayAreaCity('Sunnyvale', 37.3688, -122.0363),
  BayAreaCity('Union City', 37.5934, -122.0439),
  BayAreaCity('Vallejo', 38.1041, -122.2566),
  BayAreaCity('Walnut Creek', 37.9101, -122.0652),
];

BayAreaCity? cityNamed(String? name) {
  if (name == null) return null;
  for (final c in bayAreaCities) {
    if (c.name == name) return c;
  }
  return null;
}

/// The listed city closest to a point, or null if none is within [maxMiles]
/// (e.g. the user is outside the Bay Area).
BayAreaCity? nearestCity(
  double latitude,
  double longitude, {
  double maxMiles = 15,
}) {
  BayAreaCity? best;
  var bestMiles = double.infinity;
  for (final c in bayAreaCities) {
    final d = distanceMiles(latitude, longitude, c.latitude, c.longitude);
    if (d < bestMiles) {
      best = c;
      bestMiles = d;
    }
  }
  return bestMiles <= maxMiles ? best : null;
}
