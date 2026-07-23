package dev.iury.lifeos.task.domain.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@QuarkusTest
class TaskRelationEntityTest {

    @Inject
    EntityManager entityManager;

    @Test
    @TestTransaction
    void persistsTagDependencyAndAudit() {
        Project inbox = new Project("Inbox", null, true);
        Tag tag = new Tag("casa", "#123456");
        entityManager.persist(inbox);
        entityManager.persist(tag);
        Task blocker = Task.create("A", inbox.id);
        Task blocked = Task.create("B", inbox.id);
        entityManager.persist(blocker);
        entityManager.persist(blocked);
        entityManager.persist(new TaskTag(blocked.id, tag.id));
        entityManager.persist(new TaskDependency(blocker.id, blocked.id, DependencyType.BLOCKS));
        entityManager.persist(new ActivityLog(blocked.id, null, TaskStatus.TODO, null,
                LocalDateTime.of(2026, 7, 22, 10, 0)));
        entityManager.flush();
        assertEquals(1L,
                entityManager.createQuery("select count(t) from TaskTag t", Long.class).getSingleResult());
    }
}
