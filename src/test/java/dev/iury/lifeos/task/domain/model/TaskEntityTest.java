package dev.iury.lifeos.task.domain.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@QuarkusTest
class TaskEntityTest {

    @Inject
    EntityManager entityManager;

    @Test
    @TestTransaction
    void persistsTaskWithSpecifiedDefaults() {
        Project inbox = new Project("Inbox", null, true);
        entityManager.persist(inbox);
        Task task = Task.create("Ler a Spec", inbox.id);
        entityManager.persist(task);
        entityManager.flush();
        assertEquals(TaskStatus.TODO, task.status);
        assertEquals(TaskPriority.P4, task.priority);
        assertEquals("America/Sao_Paulo", task.timezone);
        assertEquals(1, task.version);
        assertFalse(task.completionEventPublished);
    }
}
