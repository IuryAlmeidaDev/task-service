package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import dev.iury.lifeos.task.domain.model.RecurrenceException;
import dev.iury.lifeos.task.domain.model.RecurrenceExceptionReason;
import dev.iury.lifeos.task.domain.model.RecurrenceFrequency;
import dev.iury.lifeos.task.domain.model.RecurrenceRule;
import dev.iury.lifeos.task.domain.model.RecurrenceStrategy;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class RecurrenceRepositoryTest {

    @Inject
    RecurrenceRuleRepository rules;

    @Inject
    RecurrenceExceptionRepository exceptions;

    @Test
    @TestTransaction
    void detectsExistingExceptionDate() {
        RecurrenceRule rule = new RecurrenceRule("FREQ=DAILY", RecurrenceFrequency.DAILY, 1,
                null, null, null, null, null, null,
                RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        rules.persist(rule);
        LocalDate date = LocalDate.of(2026, 7, 23);
        exceptions.persist(new RecurrenceException(rule.id, date, RecurrenceExceptionReason.SKIPPED));

        assertTrue(exceptions.exists(rule.id, date));
    }
}
