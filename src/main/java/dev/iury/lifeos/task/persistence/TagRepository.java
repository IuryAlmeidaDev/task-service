package dev.iury.lifeos.task.persistence;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import dev.iury.lifeos.task.domain.model.Tag;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Sort;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TagRepository implements PanacheRepositoryBase<Tag, UUID> {

    public Optional<Tag> findByName(String name) {
        return find("name", name).firstResultOptional();
    }

    public List<Tag> listOrdered() {
        return findAll(Sort.by("name")).list();
    }
}
