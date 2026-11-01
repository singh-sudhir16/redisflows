package io.redisflow.lock;

import io.redisflow.lock.DistributedLockService.LockHandle;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lease-based leader election on top of {@link DistributedLockService}. Call {@link #tick()}
 * periodically (well within the lease TTL): followers try to acquire the lease, the leader
 * renews it. If a renewal fails (lease lost or Redis unreachable) the node steps down at once.
 */
public class LeaderElection {

    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    private final DistributedLockService locks;
    private final String resource;
    private final String candidateId;
    private final Duration leaseTtl;
    private volatile LockHandle lease;

    public LeaderElection(DistributedLockService locks, String resource, String candidateId, Duration leaseTtl) {
        this.locks = locks;
        this.resource = resource;
        this.candidateId = candidateId;
        this.leaseTtl = leaseTtl;
    }

    /** Returns true if this node is the leader after the tick. */
    public boolean tick() {
        LockHandle current = lease;
        try {
            if (current != null) {
                if (locks.renew(resource, candidateId, leaseTtl)) {
                    return true;
                }
                log.warn("Lost leadership of '{}' (lease expired or taken over)", resource);
                lease = null;
                return false;
            }
            Optional<LockHandle> acquired = locks.tryAcquire(resource, candidateId, leaseTtl);
            acquired.ifPresent(handle -> log.info("Became leader of '{}' (fencing token {})", resource,
                    handle.fencingToken()));
            lease = acquired.orElse(null);
            return lease != null;
        } catch (RuntimeException e) {
            if (lease != null) {
                log.warn("Stepping down as leader of '{}': {}", resource, e.getMessage());
            }
            lease = null;
            return false;
        }
    }

    public boolean isLeader() {
        return lease != null;
    }

    /** Gives up leadership voluntarily (e.g. on shutdown) so another node can take over immediately. */
    public void resign() {
        LockHandle current = lease;
        lease = null;
        if (current != null) {
            try {
                locks.release(current);
            } catch (RuntimeException e) {
                log.debug("Could not release leadership lease: {}", e.getMessage());
            }
        }
    }
}
