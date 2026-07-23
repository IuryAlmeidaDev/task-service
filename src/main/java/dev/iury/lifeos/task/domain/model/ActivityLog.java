package dev.iury.lifeos.task.domain.model;

import java.time.LocalDateTime;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

@Entity
@Table(name = "activity_logs")
public class ActivityLog extends BaseEntity {

    @Column(name = "task_id", nullable = false)
    public UUID taskId;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status", length = 20)
    public TaskStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false, length = 20)
    public TaskStatus toStatus;

    @Column(length = 1000)
    public String reason;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    public LocalDateTime timestamp;

    protected ActivityLog() {
    }

    public ActivityLog(UUID taskId, TaskStatus from, TaskStatus to, String reason, LocalDateTime timestamp) {
        this.taskId = taskId;
        this.fromStatus = from;
        this.toStatus = to;
        this.reason = reason;
        this.timestamp = timestamp;
    }
}
