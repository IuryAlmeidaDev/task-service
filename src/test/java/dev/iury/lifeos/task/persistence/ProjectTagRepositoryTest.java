package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import dev.iury.lifeos.task.domain.model.Project;
import dev.iury.lifeos.task.domain.model.Tag;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

@QuarkusTest
class ProjectTagRepositoryTest {

    @Inject
    ProjectRepository projects;

    @Inject
    TagRepository tags;

    @Test
    @TestTransaction
    void findsInboxAndTagByExactName() {
        projects.persist(new Project("Inbox", null, true));
        tags.persist(new Tag("casa", "#123456"));

        assertEquals("Inbox", projects.findInbox().orElseThrow().name);
        assertEquals("casa", tags.findByName("casa").orElseThrow().name);
    }

    @Test
    @TestTransaction
    void listsProjectsOrderedByName() {
        projects.persist(new Project("Trabalho", null, false));
        projects.persist(new Project("Casa", null, false));

        assertEquals(
                java.util.List.of("Casa", "Trabalho"),
                projects.listOrdered().stream().map(project -> project.name).toList());
    }

    @Test
    @TestTransaction
    void listsTagsOrderedByName() {
        tags.persist(new Tag("trabalho", "#654321"));
        tags.persist(new Tag("casa", "#123456"));

        assertEquals(
                java.util.List.of("casa", "trabalho"),
                tags.listOrdered().stream().map(tag -> tag.name).toList());
    }
}
