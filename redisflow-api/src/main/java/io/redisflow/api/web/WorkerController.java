package io.redisflow.api.web;

import io.redisflow.analytics.WorkerGeoIndex;
import io.redisflow.analytics.WorkerGeoIndex.WorkerLocation;
import io.redisflow.worker.WorkerRegistry;
import io.redisflow.worker.WorkerRegistry.WorkerInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Workers", description = "Live workers (heartbeats) and their geographic locations")
public class WorkerController {

    public record LocationRequest(@NotNull @DecimalMin("-85.05") @DecimalMax("85.05") Double lat,
                                  @NotNull @DecimalMin("-180") @DecimalMax("180") Double lon) {
    }

    private static final Duration LIVENESS_WINDOW = Duration.ofSeconds(30);

    private final WorkerRegistry registry;
    private final WorkerGeoIndex geo;

    public WorkerController(WorkerRegistry registry, WorkerGeoIndex geo) {
        this.registry = registry;
        this.geo = geo;
    }

    @GetMapping("/workers")
    @Operation(summary = "Workers that sent a heartbeat in the last 30 seconds")
    public Map<String, List<WorkerInfo>> liveWorkers() {
        return Map.of("workers", registry.live(LIVENESS_WINDOW));
    }

    @PostMapping("/geo/workers/{workerId}")
    public Map<String, Object> register(@PathVariable String workerId, @Valid @RequestBody LocationRequest location) {
        geo.register(workerId, location.lat(), location.lon());
        return Map.of("worker_id", workerId, "lat", location.lat(), "lon", location.lon());
    }

    @GetMapping("/geo/workers")
    public Map<String, List<WorkerLocation>> allLocations() {
        return Map.of("workers", geo.all());
    }

    @GetMapping("/geo/nearby")
    @Operation(summary = "Workers within a radius, nearest first (GEOSEARCH)")
    public Map<String, Object> nearby(@RequestParam double lat, @RequestParam double lon,
                                      @RequestParam @Positive double radius,
                                      @RequestParam(defaultValue = "km") String unit) {
        List<WorkerLocation> workers = geo.nearby(lat, lon, radius, unit);
        return Map.of("workers", workers, "count", workers.size());
    }
}
