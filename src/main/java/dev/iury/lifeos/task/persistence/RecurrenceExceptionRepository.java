package dev.iury.lifeos.task.persistence;

import java.time.LocalDate;
import java.util.UUID;

import dev.iury.lifeos.task.domain.model.RecurrenceException;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RecurrenceExceptionRepository implements PanacheRepositoryBase<RecurrenceException, UUID> {

    public boolean exists(UUID ruleId, LocalDate date) {
        return count("recurrenceRuleId = ?1 and exceptionDate = ?2", ruleId, date) > 0;
    }
}
