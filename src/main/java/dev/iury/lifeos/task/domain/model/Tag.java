package dev.iury.lifeos.task.domain.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "tags")
public class Tag extends BaseEntity {

    @Column(nullable = false, unique = true, length = 50)
    public String name;

    @Column(length = 7)
    public String color;

    protected Tag() {
    }

    public Tag(String name, String color) {
        this.name = name;
        this.color = color;
    }
}
