package io.redisflow.api.web;

import io.redisflow.job.JobExceptions.IdempotencyConflictException;
import io.redisflow.job.JobExceptions.InvalidJobException;
import io.redisflow.job.JobExceptions.JobNotFoundException;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Maps domain errors to RFC 9457 problem details ({@code application/problem+json}). */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(JobNotFoundException.class)
    ProblemDetail notFound(JobNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Job not found", e.getMessage());
    }

    @ExceptionHandler({InvalidJobException.class, IllegalArgumentException.class, ConstraintViolationException.class})
    ProblemDetail badRequest(RuntimeException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    ProblemDetail idempotencyConflict(IdempotencyConflictException e) {
        ProblemDetail problem = problem(HttpStatus.UNPROCESSABLE_CONTENT, "Idempotency key reused", e.getMessage());
        problem.setProperty("existing_job_id", e.existingJobId());
        return problem;
    }

    @ExceptionHandler(DataAccessException.class)
    ProblemDetail redisUnavailable(DataAccessException e) {
        log.error("Redis operation failed", e);
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Backing store unavailable", "Please retry shortly");
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        return problem;
    }
}
