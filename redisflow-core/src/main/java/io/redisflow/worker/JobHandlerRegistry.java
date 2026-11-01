package io.redisflow.worker;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Maps job types to handlers. Fails fast at startup if two handlers claim the same type. */
public final class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public JobHandlerRegistry(Collection<? extends JobHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toUnmodifiableMap(JobHandler::type, Function.identity(),
                (a, b) -> {
                    throw new IllegalStateException("Duplicate handlers for job type '" + a.type() + "': "
                            + a.getClass().getName() + " and " + b.getClass().getName());
                }));
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return handlers.keySet();
    }
}
