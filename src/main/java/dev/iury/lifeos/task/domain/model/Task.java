package dev.iury.lifeos.task.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "tasks")
public class Task extends BaseEntity {

    @Column(nullable = false, length = 255)
    public String title;

    @Column(columnDefinition = "text")
    public String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    public TaskStatus status = TaskStatus.TODO;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 2)
    public TaskPriority priority = TaskPriority.P4;

    @Column(name = "start_date")
    public LocalDateTime startDate;

    @Column(name = "due_date")
    public LocalDateTime dueDate;

    @Column(name = "due_time")
    public LocalTime dueTime;

    @Column(nullable = false, length = 64)
    public String timezone = "America/Sao_Paulo";

    @Column(name = "estimated_duration")
    public Integer estimatedDuration;

    @Column(name = "requires_payment", nullable = false)
    public boolean requiresPayment;

    @Column(name = "expected_amount", precision = 19, scale = 2)
    public BigDecimal expectedAmount;

    @Column(name = "completion_event_published", nullable = false)
    public boolean completionEventPublished;

    @Column(name = "project_id", nullable = false)
    public UUID projectId;

    @Column(name = "parent_task_id")
    public UUID parentTaskId;

    @Column(name = "recurrence_rule_id")
    public UUID recurrenceRuleId;

    @Column(name = "parent_recurring_task_id")
    public UUID parentRecurringTaskId;

    @Column(name = "is_recurring_master", nullable = false)
    public boolean recurringMaster;

    @Version
    @Column(nullable = false)
    public Integer version = 1;

    @Column(name = "completed_at")
    public LocalDateTime completedAt;

    @Column(name = "deleted_at")
    public LocalDateTime deletedAt;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    public LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    public LocalDateTime updatedAt;

    protected Task() {
    }

    public static Task create(String title, UUID projectId) {
        Task task = new Task();
        task.title = title;
        task.projectId = projectId;
        return task;
    }
}
