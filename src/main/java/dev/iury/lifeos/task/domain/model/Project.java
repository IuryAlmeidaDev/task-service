package dev.iury.lifeos.task.domain.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "projects")
public class Project extends BaseEntity {

    @Column(nullable = false, length = 100)
    public String name;

    @Column(columnDefinition = "text")
    public String description;

    @Column(name = "is_inbox", nullable = false)
    public boolean inbox;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    public LocalDateTime createdAt;

    protected Project() {
    }

    public Project(String name, String description, boolean inbox) {
        this.name = name;
        this.description = description;
        this.inbox = inbox;
    }
}
