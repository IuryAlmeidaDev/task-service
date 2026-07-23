package dev.iury.lifeos.task.domain.model;

import java.util.UUID;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "task_tags")
public class TaskTag {

    @EmbeddedId
    public TaskTagId id;

    protected TaskTag() {
    }

    public TaskTag(UUID taskId, UUID tagId) {
        this.id = new TaskTagId(taskId, tagId);
    }
}
