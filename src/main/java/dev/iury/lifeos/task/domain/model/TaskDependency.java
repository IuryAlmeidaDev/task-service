package dev.iury.lifeos.task.domain.model;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

@Entity
@Table(name = "task_dependencies")
public class TaskDependency extends BaseEntity {

    @Column(name = "blocking_task_id", nullable = false)
    public UUID blockingTaskId;

    @Column(name = "blocked_task_id", nullable = false)
    public UUID blockedTaskId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    public DependencyType type;

    protected TaskDependency() {
    }

    public TaskDependency(UUID blockingId, UUID blockedId, DependencyType type) {
        this.blockingTaskId = blockingId;
        this.blockedTaskId = blockedId;
        this.type = type;
    }
}
