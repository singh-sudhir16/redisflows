package io.redisflow.analytics;

import io.redisflow.redis.RedisKeys;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;

/** Worker locations in a Redis geospatial index (GEOADD / GEOSEARCH) for region-aware routing. */
public class WorkerGeoIndex {

    public record WorkerLocation(String workerId, double lat, double lon, Double distance, String unit) {
    }

    private final StringRedisTemplate redis;
    private final RedisKeys keys;

    public WorkerGeoIndex(StringRedisTemplate redis, RedisKeys keys) {
        this.redis = redis;
        this.keys = keys;
    }

    public void register(String workerId, double lat, double lon) {
        if (lat < -85.05112878 || lat > 85.05112878 || lon < -180 || lon > 180) {
            throw new IllegalArgumentException("lat must be within +/-85.05 and lon within +/-180");
        }
        redis.opsForGeo().add(keys.workerGeo(), new Point(lon, lat), workerId);
    }

    public void remove(String workerId) {
        redis.opsForGeo().remove(keys.workerGeo(), workerId);
    }

    public List<WorkerLocation> all() {
        Set<String> members = redis.opsForZSet().range(keys.workerGeo(), 0, -1);
        if (members == null || members.isEmpty()) {
            return List.of();
        }
        List<String> ids = new ArrayList<>(members);
        List<Point> points = redis.opsForGeo().position(keys.workerGeo(), ids.toArray(String[]::new));
        List<WorkerLocation> result = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            Point p = points == null ? null : points.get(i);
            if (p != null) {
                result.add(new WorkerLocation(ids.get(i), p.getY(), p.getX(), null, null));
            }
        }
        return result;
    }

    public List<WorkerLocation> nearby(double lat, double lon, double radius, String unit) {
        DistanceUnit distanceUnit = parseUnit(unit);
        GeoResults<GeoLocation<String>> results = redis.opsForGeo().search(keys.workerGeo(),
                GeoReference.fromCoordinate(lon, lat), new Distance(radius, distanceUnit),
                GeoSearchCommandArgs.newGeoSearchArgs().includeCoordinates().includeDistance().sortAscending());
        if (results == null) {
            return List.of();
        }
        List<WorkerLocation> nearby = new ArrayList<>();
        for (GeoResult<GeoLocation<String>> r : results) {
            Point p = r.getContent().getPoint();
            nearby.add(new WorkerLocation(r.getContent().getName(), p.getY(), p.getX(), r.getDistance().getValue(),
                    distanceUnit.getAbbreviation()));
        }
        return nearby;
    }

    private static DistanceUnit parseUnit(String unit) {
        return switch (unit == null ? "km" : unit.toLowerCase(Locale.ROOT)) {
            case "m" -> DistanceUnit.METERS;
            case "km" -> DistanceUnit.KILOMETERS;
            case "mi" -> DistanceUnit.MILES;
            case "ft" -> DistanceUnit.FEET;
            default -> throw new IllegalArgumentException("unit must be one of m, km, mi, ft");
        };
    }
}
