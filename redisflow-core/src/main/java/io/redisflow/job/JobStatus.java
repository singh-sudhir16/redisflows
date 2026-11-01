package io.redisflow.job;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

/** Job lifecycle: scheduled -> pending -> processing -> completed | retrying -> ... | dead. */
public enum JobStatus {
    SCHEDULED,
    PENDING,
    PROCESSING,
    RETRYING,
    COMPLETED,
    DEAD;

    @JsonValue
    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == DEAD;
    }

    public static JobStatus fromValue(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
