package io.redisflow.api.web;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.lock.DistributedLockService;
import io.redisflow.lock.DistributedLockService.LockHandle;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.time.Duration;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/locks")
@Tag(name = "Locks", description = "Distributed locks with TTL and fencing tokens")
public class LockController {

    private static final String RESOURCE = "^[A-Za-z0-9_.:-]{1,128}$";

    public record AcquireRequest(@NotBlank String holder, @Min(100) @Max(3_600_000) Long ttlMs) {
    }

    private final DistributedLockService locks;
    private final Duration defaultTtl;

    public LockController(DistributedLockService locks, RedisFlowProperties props) {
        this.locks = locks;
        this.defaultTtl = props.locks().defaultTtl();
    }

    @GetMapping("/{resource}")
    public ResponseEntity<?> inspect(@PathVariable @Pattern(regexp = RESOURCE) String resource) {
        return locks.inspect(resource)
                .<ResponseEntity<?>>map(info -> ResponseEntity.ok(Map.of(
                        "resource", resource, "held", true, "holder", info.holder(),
                        "ttl_ms", info.remainingTtl().toMillis(), "last_fencing_token", info.lastFencingToken())))
                .orElseGet(() -> ResponseEntity.ok(Map.of("resource", resource, "held", false)));
    }

    @PostMapping("/{resource}")
    @Operation(summary = "Try to acquire a lock; returns a fencing token, or 409 if it is held")
    public ResponseEntity<?> acquire(@PathVariable @Pattern(regexp = RESOURCE) String resource,
                                     @Valid @RequestBody AcquireRequest request) {
        Duration ttl = request.ttlMs() == null ? defaultTtl : Duration.ofMillis(request.ttlMs());
        return locks.tryAcquire(resource, request.holder(), ttl)
                .<ResponseEntity<?>>map(LockController::acquired)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(Map.of("resource", resource, "acquired", false)));
    }

    @DeleteMapping("/{resource}")
    @Operation(summary = "Release a lock; only the current holder can release it")
    public ResponseEntity<?> release(@PathVariable @Pattern(regexp = RESOURCE) String resource,
                                     @RequestParam String holder) {
        boolean released = locks.release(resource, holder);
        return ResponseEntity.status(released ? HttpStatus.OK : HttpStatus.CONFLICT)
                .body(Map.of("resource", resource, "released", released));
    }

    private static ResponseEntity<?> acquired(LockHandle handle) {
        return ResponseEntity.ok(Map.of("resource", handle.resource(), "acquired", true, "holder", handle.holder(),
                "fencing_token", handle.fencingToken(), "ttl_ms", handle.ttl().toMillis()));
    }
}
