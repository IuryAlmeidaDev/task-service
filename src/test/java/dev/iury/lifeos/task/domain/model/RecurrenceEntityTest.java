package dev.iury.lifeos.task.domain.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@QuarkusTest
class RecurrenceEntityTest {

    @Inject
    EntityManager entityManager;

    @Test
    @TestTransaction
    void persistsRuleAndSkippedDate() {
        RecurrenceRule rule = new RecurrenceRule(
                "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU,TH",
                RecurrenceFrequency.WEEKLY, 2, "TU,TH", null, null, null,
                null, null, RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        entityManager.persist(rule);
        RecurrenceException exception = new RecurrenceException(
                rule.id, LocalDate.of(2026, 7, 23), RecurrenceExceptionReason.SKIPPED);
        entityManager.persist(exception);
        entityManager.flush();
        assertNotNull(rule.id);
        assertNotNull(exception.id);
    }
}
