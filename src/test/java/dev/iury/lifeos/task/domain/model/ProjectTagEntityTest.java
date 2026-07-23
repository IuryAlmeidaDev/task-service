package dev.iury.lifeos.task.domain.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

@QuarkusTest
class ProjectTagEntityTest {

    @Inject
    EntityManager entityManager;

    @Test
    @TestTransaction
    void persistsProjectAndTagWithGeneratedIds() {
        Project project = new Project("Inbox", null, true);
        Tag tag = new Tag("urgente", "#FF5733");
        entityManager.persist(project);
        entityManager.persist(tag);
        entityManager.flush();
        assertNotNull(project.id);
        assertNotNull(project.createdAt);
        assertNotNull(tag.id);
    }
}
