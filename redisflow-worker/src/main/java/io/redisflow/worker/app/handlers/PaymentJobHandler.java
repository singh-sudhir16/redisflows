package io.redisflow.worker.app.handlers;

import io.redisflow.job.Job;
import io.redisflow.worker.JobHandler;
import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Simulated payment capture. Shows the pattern a real integration needs: validate up front
 * (bad input is not retryable) and pass the job id to the payment provider as the idempotency
 * key, so an at-least-once redelivery can never charge the customer twice.
 */
@Component
public class PaymentJobHandler implements JobHandler {

    record PaymentPayload(BigDecimal amount, String currency, String orderId) {
    }

    private static final Logger log = LoggerFactory.getLogger(PaymentJobHandler.class);
    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");

    private final JsonMapper json;

    public PaymentJobHandler(JsonMapper json) {
        this.json = json;
    }

    @Override
    public String type() {
        return "payment";
    }

    @Override
    public void handle(Job job) throws InterruptedException {
        PaymentPayload payment = Payloads.read(json, job, PaymentPayload.class);
        Payloads.require(payment.amount() != null && payment.amount().signum() > 0, "amount must be positive");
        Payloads.require(payment.currency() != null && CURRENCY.matcher(payment.currency()).matches(),
                "currency must be an ISO-4217 code");

        Thread.sleep(ThreadLocalRandom.current().nextLong(50, 150)); // provider round trip
        log.info("[payment] captured {} {} order={} idempotency_key={}", payment.amount(), payment.currency(),
                payment.orderId(), job.id());
    }
}
