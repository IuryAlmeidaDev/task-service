package dev.iury.lifeos.task.persistence;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import dev.iury.lifeos.task.domain.model.Project;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Sort;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ProjectRepository implements PanacheRepositoryBase<Project, UUID> {

    public Optional<Project> findInbox() {
        return find("inbox", true).firstResultOptional();
    }

    public List<Project> listOrdered() {
        return findAll(Sort.by("name")).list();
    }
}
