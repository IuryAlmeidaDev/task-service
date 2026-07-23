package dev.iury.lifeos.task.domain.model;

import java.io.Serializable;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public record TaskTagId(
        @Column(name = "task_id") UUID taskId,
        @Column(name = "tag_id") UUID tagId) implements Serializable {
}
