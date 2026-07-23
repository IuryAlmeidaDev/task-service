package dev.iury.lifeos.task.application;

import java.time.LocalDateTime;
import java.util.UUID;

import dev.iury.lifeos.task.domain.model.TaskPriority;
import dev.iury.lifeos.task.domain.model.TaskStatus;

public record TaskFilter(
        TaskStatus status,
        TaskPriority priority,
        UUID projectId,
        UUID tagId,
        LocalDateTime dueFrom,
        LocalDateTime dueTo) {
}
