package dev.iury.lifeos.task.persistence;

import java.util.UUID;

import dev.iury.lifeos.task.domain.model.RecurrenceRule;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RecurrenceRuleRepository implements PanacheRepositoryBase<RecurrenceRule, UUID> {
}
