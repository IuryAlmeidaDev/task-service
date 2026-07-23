package dev.iury.lifeos.task.domain.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "recurrence_exceptions",
        uniqueConstraints = @UniqueConstraint(columnNames = {"recurrence_rule_id", "exception_date"}))
public class RecurrenceException extends BaseEntity {

    @Column(name = "recurrence_rule_id", nullable = false)
    public UUID recurrenceRuleId;

    @Column(name = "exception_date", nullable = false)
    public LocalDate exceptionDate;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    public RecurrenceExceptionReason reason;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    public LocalDateTime createdAt;

    protected RecurrenceException() {
    }

    public RecurrenceException(UUID ruleId, LocalDate date, RecurrenceExceptionReason reason) {
        this.recurrenceRuleId = ruleId;
        this.exceptionDate = date;
        this.reason = reason;
    }
}
