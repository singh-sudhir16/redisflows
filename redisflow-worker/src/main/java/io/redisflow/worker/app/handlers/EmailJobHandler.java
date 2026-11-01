package io.redisflow.worker.app.handlers;

import io.redisflow.job.Job;
import io.redisflow.worker.JobHandler;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Sends an email through SMTP when {@code spring.mail.host} is configured (credentials come
 * from the environment, never from source); otherwise logs the message so the demo runs anywhere.
 */
@Component
public class EmailJobHandler implements JobHandler {

    record EmailPayload(String to, String subject, String body) {
    }

    private static final Logger log = LoggerFactory.getLogger(EmailJobHandler.class);
    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private final JsonMapper json;
    private final ObjectProvider<JavaMailSender> mailSender;
    private final String from;

    public EmailJobHandler(JsonMapper json, ObjectProvider<JavaMailSender> mailSender,
                           @Value("${redisflow.mail.from:redisflow@localhost}") String from) {
        this.json = json;
        this.mailSender = mailSender;
        this.from = from;
    }

    @Override
    public String type() {
        return "email";
    }

    @Override
    public void handle(Job job) {
        EmailPayload email = Payloads.read(json, job, EmailPayload.class);
        Payloads.require(email.to() != null && EMAIL.matcher(email.to()).matches(), "a valid 'to' address is required");

        JavaMailSender sender = mailSender.getIfAvailable();
        if (sender == null) {
            log.info("[email:simulated] job={} to={} subject={}", job.id(), email.to(), email.subject());
            return;
        }
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(from);
        message.setTo(email.to());
        message.setSubject(email.subject() == null ? "(no subject)" : email.subject());
        message.setText(email.body() == null ? "" : email.body());
        sender.send(message); // MailException -> retried with backoff
        log.info("[email] job={} sent to={}", job.id(), email.to());
    }
}
