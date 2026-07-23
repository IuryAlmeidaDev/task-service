# Task Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o Task Service completo em Java 21 e Quarkus, persistindo tarefas em `tasks_db`, aplicando as regras de ciclo de vida, recorrência, subtarefas, dependências e lixeira, e publicando eventos de conclusão/reabertura no Kafka.

**Architecture:** Aplicação Quarkus síncrona, organizada em API, aplicação/domínio, persistência e integrações. Entidades Panache não são expostas pela API; resources recebem records de request, serviços transacionais aplicam regras, repositories concentram consultas, e mappers produzem records de response. PostgreSQL é a fonte de verdade, Flyway é o único mecanismo de schema, e o Kafka é acessado por um publisher dedicado.

**Tech Stack:** Java 21, Quarkus 3.37.3, Quarkus REST/Jackson, Hibernate ORM with Panache, PostgreSQL, Flyway, SmallRye Reactive Messaging Kafka, SmallRye Fault Tolerance, Scheduler, ical4j 4.2.5, jsoup 1.22.2, JUnit 5, REST Assured e Quarkus Dev Services/Testcontainers.

## Global Constraints

- Implementar somente o Task Service; não criar Nginx, PostgreSQL, Kafka nem código do Finance Service.
- Usar Java 21 e Quarkus 3.37.3; o serviço HTTP escuta na porta `8081`.
- Usar o banco PostgreSQL isolado `tasks_db`; Hibernate deve validar o schema e Flyway deve criá-lo.
- Publicar `TaskCompletedEvent` em `task.completed.events` e `TaskReopenedEvent` em `task.reopened.events` com os payloads da Spec.
- Persistir enums como texto e UUIDs nativos do PostgreSQL.
- Não expor entidades JPA em recursos REST.
- Toda escrita ocorre em transação; toda alteração de status cria `ActivityLog` imutável.
- Cada tarefa deste plano termina com teste focado verde e commit próprio.
- Todo comando PowerShell deste workspace deve ser executado com o prefixo `rtk`.

## Decisões que fecham ambiguidades da Spec

1. O MVP é single-user, porque as Specs não definem autenticação nem identidade do usuário. Assim, existe um único Inbox e `Tag.name` é único globalmente; isso satisfaz “por usuário” para o único usuário sem inventar headers ou autenticação.
2. `subtaskCompletionMode` não será criado: a seção 6 define `STRICT` como comportamento padrão e não especifica outro valor. Pai só conclui quando cada filha ativa está `COMPLETED` ou `CANCELED`.
3. Clones recorrentes mantêm `isRecurringMaster=true` para que a série continue. `parentRecurringTaskId` sempre aponta para a primeira tarefa da série; a primeira tarefa mantém esse campo nulo.
4. `count` é verificado contando a tarefa raiz e seus clones. `until` é comparado no timezone da regra.
5. O endpoint `skip` registra como `exceptionDate` a data local da ocorrência em `dueDate`, e não a data do relógio do servidor.
6. `OVERDUE` pode ir para `IN_PROGRESS`, `COMPLETED` ou `CANCELED`; sem isso uma tarefa atrasada não poderia ser concluída.
7. Uma tarefa `requiresPayment=true` concluída publica o evento de forma síncrona com retry e só então marca o booleano técnico interno `completionEventPublished=true`. A reabertura consulta esse marcador, pois a Spec exige compensação apenas quando o evento anterior foi emitido. O marcador não é exposto na API; não será criada outbox porque a Spec não a pede.
8. Exclusão em cascata usa o mesmo valor de `deletedAt` para pai e descendentes. A restauração usa esse valor para restaurar somente o lote apagado junto.
9. A listagem usa `page=0`, `size=20` e máximo `size=100`, ordenada por `dueDate ASC NULLS LAST, createdAt DESC`.
10. Datas da API usam ISO-8601. `LocalDateTime` segue o timezone IANA da tarefa; timestamps Kafka são convertidos para `Instant`/UTC.

## Mapa de arquivos e responsabilidades

```text
task-service/
├── pom.xml                                      # build e dependências
├── README.md                                    # execução, configuração e API
├── src/main/resources/application.properties   # runtime, DB, Flyway e Kafka
├── src/main/resources/db/migration/
│   ├── V1__create_reference_tables.sql          # projects, tags, recurrence
│   └── V2__create_task_tables.sql               # tasks, N:N, DAG e auditoria
├── src/main/java/dev/iury/lifeos/task/
│   ├── domain/model/                            # uma entidade ou enum por arquivo
│   ├── domain/error/                            # uma exceção de negócio por arquivo
│   ├── domain/validation/                       # validação e sanitização puras
│   ├── persistence/                             # um repository Panache por agregado
│   ├── application/                             # serviços transacionais e results
│   ├── recurrence/                              # parser e cálculo RRULE
│   ├── messaging/                               # records de evento e publisher
│   ├── job/                                     # OverdueChecker e TrashPurger
│   └── api/
│       ├── dto/                                 # um request/response por arquivo
│       ├── mapper/TaskMapper.java
│       ├── error/                               # ErrorResponse e exception mappers
│       └── resource/                            # resources REST por recurso
└── src/test/java/dev/iury/lifeos/task/           # espelha o pacote de produção
```

---

### Task 1: Bootstrap reproduzível do Quarkus

**Files:**
- Create: `pom.xml`
- Create: `.gitignore`
- Create: `src/main/resources/application.properties`
- Test: `src/test/java/dev/iury/lifeos/task/BootstrapTest.java`

**Interfaces:**
- Produces: aplicação Quarkus Java 21; profiles `%dev`, `%test` e `%prod`; Maven Wrapper.

- [ ] **Step 1: criar apenas o build e escrever o teste falho**

Crie o `pom.xml` com BOM `io.quarkus.platform:quarkus-bom:3.37.3`, `maven.compiler.release=21` e estas dependências exatas: `quarkus-arc`, `quarkus-rest-jackson`, `quarkus-hibernate-orm-panache`, `quarkus-jdbc-postgresql`, `quarkus-flyway`, `quarkus-messaging-kafka`, `quarkus-smallrye-fault-tolerance`, `quarkus-scheduler`, `quarkus-hibernate-validator`, `org.mnode.ical4j:ical4j:4.2.5`, `org.jsoup:jsoup:1.22.2`, `quarkus-junit5` (test), `quarkus-junit-mockito` (test), `org.junit.jupiter:junit-jupiter-params` (test) e `io.rest-assured:rest-assured` (test). Configure `io.quarkus.platform:quarkus-maven-plugin:3.37.3` com `<extensions>true</extensions>`; `@TestTransaction` já é fornecido pelo suporte de teste Quarkus.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.iury.lifeos</groupId>
  <artifactId>task-service</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <quarkus.platform.version>3.37.3</quarkus.platform.version>
    <skipITs>true</skipITs>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>io.quarkus.platform</groupId>
        <artifactId>quarkus-bom</artifactId>
        <version>${quarkus.platform.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-arc</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-rest-jackson</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-hibernate-orm-panache</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-jdbc-postgresql</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-flyway</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-messaging-kafka</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-smallrye-fault-tolerance</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-scheduler</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-hibernate-validator</artifactId></dependency>
    <dependency><groupId>org.mnode.ical4j</groupId><artifactId>ical4j</artifactId><version>4.2.5</version></dependency>
    <dependency><groupId>org.jsoup</groupId><artifactId>jsoup</artifactId><version>1.22.2</version></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit5</artifactId><scope>test</scope></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit-mockito</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter-params</artifactId><scope>test</scope></dependency>
    <dependency><groupId>io.rest-assured</groupId><artifactId>rest-assured</artifactId><scope>test</scope></dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>io.quarkus.platform</groupId>
        <artifactId>quarkus-maven-plugin</artifactId>
        <version>${quarkus.platform.version}</version>
        <extensions>true</extensions>
        <executions><execution><goals><goal>build</goal><goal>generate-code</goal><goal>generate-code-tests</goal></goals></execution></executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.14.1</version>
        <configuration><parameters>true</parameters></configuration>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.5.4</version>
        <configuration><systemPropertyVariables><java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager></systemPropertyVariables></configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

Depois rode `rtk mvn -N wrapper:wrapper` e crie:

```java
package dev.iury.lifeos.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

@QuarkusTest
class BootstrapTest {
    @Test
    void usesJava21AndTaskServiceName() {
        assertEquals("21", System.getProperty("java.specification.version"));
        assertEquals("task-service", ConfigProvider.getConfig()
                .getValue("quarkus.application.name", String.class));
        assertEquals(8081, ConfigProvider.getConfig().getValue("quarkus.http.port", Integer.class));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=BootstrapTest" test`

Expected: FAIL porque `quarkus.http.port=8081` ainda não existe.

- [ ] **Step 3: adicionar a configuração mínima**

```properties
quarkus.application.name=task-service
quarkus.http.port=8081
quarkus.datasource.db-kind=postgresql
quarkus.datasource.devservices.db-name=tasks_db
quarkus.hibernate-orm.schema-management.strategy=validate
quarkus.flyway.migrate-at-start=true

%prod.quarkus.datasource.username=${TASKS_DB_USERNAME:lifeos}
%prod.quarkus.datasource.password=${TASKS_DB_PASSWORD:lifeos}
%prod.quarkus.datasource.jdbc.url=${TASKS_DB_URL:jdbc:postgresql://localhost:5432/tasks_db}
%prod.kafka.bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

mp.messaging.outgoing.task-completed.connector=smallrye-kafka
mp.messaging.outgoing.task-completed.topic=task.completed.events
mp.messaging.outgoing.task-completed.value.serializer=io.quarkus.kafka.client.serialization.ObjectMapperSerializer
mp.messaging.outgoing.task-reopened.connector=smallrye-kafka
mp.messaging.outgoing.task-reopened.topic=task.reopened.events
mp.messaging.outgoing.task-reopened.value.serializer=io.quarkus.kafka.client.serialization.ObjectMapperSerializer
```

Use `.gitignore` com `target/`, `.idea/`, `.vscode/`, `*.iml` e `.env`.

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=BootstrapTest" test`

Expected: PASS, 1 test, 0 failures.

- [ ] **Step 5: commit**

```powershell
rtk git add pom.xml .mvn mvnw mvnw.cmd .gitignore src/main/resources/application.properties src/test/java/dev/iury/lifeos/task/BootstrapTest.java
rtk git commit -m "build: bootstrap Quarkus task service"
```

### Task 2: Migration das tabelas de referência

**Files:**
- Create: `src/main/resources/db/migration/V1__create_reference_tables.sql`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/ReferenceSchemaMigrationTest.java`

**Interfaces:**
- Produces: tabelas `projects`, `tags`, `recurrence_rules` e `recurrence_exceptions`.

- [ ] **Step 1: escrever o teste falho de schema**

```java
package dev.iury.lifeos.task.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.sql.ResultSet;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ReferenceSchemaMigrationTest {
    @Inject AgroalDataSource dataSource;

    @Test
    void createsFourReferenceTables() throws Exception {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                 select count(*) from information_schema.tables
                 where table_schema = 'public'
                   and table_name in ('projects','tags','recurrence_rules','recurrence_exceptions')
                 """);
             ResultSet result = statement.executeQuery()) {
            result.next();
            assertEquals(4, result.getInt(1));
        }
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ReferenceSchemaMigrationTest" test`

Expected: FAIL com contagem `0` em vez de `4`.

- [ ] **Step 3: criar a migration real**

```sql
CREATE TABLE projects (
    id UUID PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_inbox BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX ux_projects_single_inbox ON projects (is_inbox) WHERE is_inbox;

CREATE TABLE tags (
    id UUID PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    color VARCHAR(7),
    CONSTRAINT ck_tags_color CHECK (color IS NULL OR color ~ '^#[0-9A-Fa-f]{6}$')
);

CREATE TABLE recurrence_rules (
    id UUID PRIMARY KEY,
    rrule_string VARCHAR(1000) NOT NULL,
    freq VARCHAR(16) NOT NULL,
    recurrence_interval INTEGER NOT NULL DEFAULT 1,
    by_day VARCHAR(255),
    by_month_day INTEGER,
    by_month INTEGER,
    by_set_pos INTEGER,
    occurrence_count INTEGER,
    until_at TIMESTAMP,
    strategy VARCHAR(32) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_recurrence_freq CHECK (freq IN ('DAILY','WEEKLY','MONTHLY','YEARLY')),
    CONSTRAINT ck_recurrence_interval CHECK (recurrence_interval > 0),
    CONSTRAINT ck_recurrence_month_day CHECK (by_month_day IS NULL OR by_month_day = -1 OR by_month_day BETWEEN 1 AND 31),
    CONSTRAINT ck_recurrence_month CHECK (by_month IS NULL OR by_month BETWEEN 1 AND 12),
    CONSTRAINT ck_recurrence_count CHECK (occurrence_count IS NULL OR occurrence_count > 0),
    CONSTRAINT ck_recurrence_limit CHECK (occurrence_count IS NULL OR until_at IS NULL),
    CONSTRAINT ck_recurrence_strategy CHECK (strategy IN ('FIXED_SCHEDULE','COMPLETION_BASED'))
);

CREATE TABLE recurrence_exceptions (
    id UUID PRIMARY KEY,
    recurrence_rule_id UUID NOT NULL REFERENCES recurrence_rules(id) ON DELETE CASCADE,
    exception_date DATE NOT NULL,
    reason VARCHAR(16) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_recurrence_exception UNIQUE (recurrence_rule_id, exception_date),
    CONSTRAINT ck_recurrence_exception_reason CHECK (reason IN ('SKIPPED','RESCHEDULED'))
);
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ReferenceSchemaMigrationTest" test`

Expected: PASS; Flyway aplica a versão 1 e as quatro tabelas existem no PostgreSQL de teste.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/resources/db/migration/V1__create_reference_tables.sql src/test/java/dev/iury/lifeos/task/persistence/ReferenceSchemaMigrationTest.java
rtk git commit -m "feat: create task reference schema"
```

### Task 3: Migration de tarefas, relações e auditoria

**Files:**
- Create: `src/main/resources/db/migration/V2__create_task_tables.sql`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/TaskSchemaMigrationTest.java`

**Interfaces:**
- Consumes: tabelas da migration V1.
- Produces: `tasks`, `task_tags`, `task_dependencies`, `activity_logs` e índices de consulta/job.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskSchemaMigrationTest {
    @Inject AgroalDataSource dataSource;

    @Test
    void createsTaskTablesAndOptimisticVersion() throws Exception {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                 select count(*) from information_schema.columns
                 where table_schema='public' and table_name='tasks'
                   and column_name in ('id','status','version','deleted_at','completion_event_published')
                 """);
             var result = statement.executeQuery()) {
            result.next();
            assertEquals(5, result.getInt(1));
        }
    }
}
```

Inclua package/imports idênticos ao teste da Task 2.

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskSchemaMigrationTest" test`

Expected: FAIL com contagem `0`.

- [ ] **Step 3: criar a migration real**

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'TODO',
    priority VARCHAR(2) NOT NULL DEFAULT 'P4',
    start_date TIMESTAMP,
    due_date TIMESTAMP,
    due_time TIME,
    timezone VARCHAR(64) NOT NULL DEFAULT 'America/Sao_Paulo',
    estimated_duration INTEGER,
    requires_payment BOOLEAN NOT NULL DEFAULT FALSE,
    expected_amount NUMERIC(19,2),
    completion_event_published BOOLEAN NOT NULL DEFAULT FALSE,
    project_id UUID NOT NULL REFERENCES projects(id),
    parent_task_id UUID REFERENCES tasks(id),
    recurrence_rule_id UUID REFERENCES recurrence_rules(id),
    parent_recurring_task_id UUID REFERENCES tasks(id),
    is_recurring_master BOOLEAN NOT NULL DEFAULT FALSE,
    version INTEGER NOT NULL DEFAULT 1,
    completed_at TIMESTAMP,
    deleted_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_tasks_title CHECK (length(btrim(title)) BETWEEN 1 AND 255),
    CONSTRAINT ck_tasks_description CHECK (description IS NULL OR octet_length(description) <= 65536),
    CONSTRAINT ck_tasks_status CHECK (status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE')),
    CONSTRAINT ck_tasks_priority CHECK (priority IN ('P1','P2','P3','P4')),
    CONSTRAINT ck_tasks_dates CHECK (start_date IS NULL OR due_date IS NULL OR start_date <= due_date),
    CONSTRAINT ck_tasks_duration CHECK (estimated_duration IS NULL OR estimated_duration BETWEEN 1 AND 525600),
    CONSTRAINT ck_tasks_payment CHECK (requires_payment OR expected_amount IS NULL),
    CONSTRAINT ck_tasks_recurrence_due CHECK (recurrence_rule_id IS NULL OR due_date IS NOT NULL),
    CONSTRAINT ck_tasks_parent CHECK (parent_task_id IS NULL OR parent_task_id <> id)
);

CREATE TABLE task_tags (
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, tag_id)
);

CREATE TABLE task_dependencies (
    id UUID PRIMARY KEY,
    blocking_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    blocked_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    type VARCHAR(16) NOT NULL,
    CONSTRAINT uq_task_dependency UNIQUE (blocking_task_id, blocked_task_id),
    CONSTRAINT ck_dependency_self CHECK (blocking_task_id <> blocked_task_id),
    CONSTRAINT ck_dependency_type CHECK (type IN ('BLOCKS','RELATES_TO','DUPLICATES'))
);

CREATE TABLE activity_logs (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    from_status VARCHAR(20),
    to_status VARCHAR(20) NOT NULL,
    reason VARCHAR(1000),
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_activity_from_status CHECK (from_status IS NULL OR from_status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE')),
    CONSTRAINT ck_activity_to_status CHECK (to_status IN ('TODO','IN_PROGRESS','BLOCKED','COMPLETED','CANCELED','OVERDUE'))
);

CREATE INDEX ix_tasks_active_listing ON tasks (status, priority, project_id, due_date) WHERE deleted_at IS NULL;
CREATE INDEX ix_tasks_visible_start ON tasks (start_date) WHERE deleted_at IS NULL;
CREATE INDEX ix_tasks_parent ON tasks (parent_task_id);
CREATE INDEX ix_tasks_series ON tasks (parent_recurring_task_id);
CREATE INDEX ix_tasks_trash ON tasks (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX ix_task_dependencies_blocked ON task_dependencies (blocked_task_id, type);
CREATE INDEX ix_activity_logs_task_time ON activity_logs (task_id, occurred_at);
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskSchemaMigrationTest" test`

Expected: PASS; Flyway aplica as versões 1 e 2 sem erro.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/resources/db/migration/V2__create_task_tables.sql src/test/java/dev/iury/lifeos/task/persistence/TaskSchemaMigrationTest.java
rtk git commit -m "feat: create task persistence schema"
```

### Task 4: Entidades Project e Tag

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/BaseEntity.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/Project.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/Tag.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/model/ProjectTagEntityTest.java`

**Interfaces:**
- Produces: entidades com UUID gerado; `Project(name, description, inbox)`; `Tag(name, color)`.

- [ ] **Step 1: escrever o teste falho de persistência**

```java
@QuarkusTest
class ProjectTagEntityTest {
    @Inject EntityManager entityManager;

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
```

Use imports de JUnit, `io.quarkus.test.TestTransaction`, `@QuarkusTest`, `EntityManager` e `@Inject`.

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectTagEntityTest" test`

Expected: compilation failure porque as três entidades não existem.

- [ ] **Step 3: implementar as entidades mínimas**

```java
@MappedSuperclass
public abstract class BaseEntity {
    @Id @GeneratedValue @UuidGenerator
    public UUID id;
}
```

```java
@Entity
@Table(name = "projects")
public class Project extends BaseEntity {
    @Column(nullable = false, length = 100) public String name;
    @Column(columnDefinition = "text") public String description;
    @Column(name = "is_inbox", nullable = false) public boolean inbox;
    @CreationTimestamp @Column(name = "created_at", updatable = false) public LocalDateTime createdAt;
    protected Project() {}
    public Project(String name, String description, boolean inbox) {
        this.name = name;
        this.description = description;
        this.inbox = inbox;
    }
}
```

```java
@Entity
@Table(name = "tags")
public class Tag extends BaseEntity {
    @Column(nullable = false, unique = true, length = 50) public String name;
    @Column(length = 7) public String color;
    protected Tag() {}
    public Tag(String name, String color) {
        this.name = name;
        this.color = color;
    }
}
```

Adicione os imports Jakarta Persistence, Hibernate `@UuidGenerator`/`@CreationTimestamp`, `UUID` e `LocalDateTime` em seus respectivos arquivos.

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectTagEntityTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/model src/test/java/dev/iury/lifeos/task/domain/model/ProjectTagEntityTest.java
rtk git commit -m "feat: map project and tag entities"
```

### Task 5: Entidades de recorrência

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/RecurrenceFrequency.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/RecurrenceStrategy.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/RecurrenceExceptionReason.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/RecurrenceRule.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/RecurrenceException.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/model/RecurrenceEntityTest.java`

**Interfaces:**
- Produces: `RecurrenceRule` com todos os campos normalizados da RRULE; exceção única por regra/data.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class RecurrenceEntityTest {
    @Inject EntityManager entityManager;

    @Test
    @TestTransaction
    void persistsRuleAndSkippedDate() {
        RecurrenceRule rule = new RecurrenceRule(
                "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU,TH",
                RecurrenceFrequency.WEEKLY, 2, "TU,TH", null, null, null,
                null, null, RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        entityManager.persist(rule);
        RecurrenceException exception = new RecurrenceException(
                rule.id, LocalDate.of(2026, 7, 23), RecurrenceExceptionReason.SKIPPED);
        entityManager.persist(exception);
        entityManager.flush();
        assertNotNull(rule.id);
        assertNotNull(exception.id);
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceEntityTest" test`

Expected: compilation failure por ausência dos tipos.

- [ ] **Step 3: implementar enums e entidades**

Cada enum fica em seu próprio arquivo:

```java
public enum RecurrenceFrequency { DAILY, WEEKLY, MONTHLY, YEARLY }
public enum RecurrenceStrategy { FIXED_SCHEDULE, COMPLETION_BASED }
public enum RecurrenceExceptionReason { SKIPPED, RESCHEDULED }
```

O conteúdo funcional de `RecurrenceRule` deve ser:

```java
@Entity
@Table(name = "recurrence_rules")
public class RecurrenceRule extends BaseEntity {
    @Column(name = "rrule_string", nullable = false, length = 1000) public String rruleString;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 16) public RecurrenceFrequency freq;
    @Column(name = "recurrence_interval", nullable = false) public int interval;
    @Column(name = "by_day") public String byDay;
    @Column(name = "by_month_day") public Integer byMonthDay;
    @Column(name = "by_month") public Integer byMonth;
    @Column(name = "by_set_pos") public Integer bySetPos;
    @Column(name = "occurrence_count") public Integer count;
    @Column(name = "until_at") public LocalDateTime until;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 32) public RecurrenceStrategy strategy;
    @Column(nullable = false, length = 64) public String timezone;
    @CreationTimestamp @Column(name = "created_at", updatable = false) public LocalDateTime createdAt;
    @UpdateTimestamp @Column(name = "updated_at") public LocalDateTime updatedAt;

    protected RecurrenceRule() {}
    public RecurrenceRule(String rruleString, RecurrenceFrequency freq, int interval,
            String byDay, Integer byMonthDay, Integer byMonth, Integer bySetPos,
            Integer count, LocalDateTime until, RecurrenceStrategy strategy, String timezone) {
        this.rruleString = rruleString;
        this.freq = freq;
        this.interval = interval;
        this.byDay = byDay;
        this.byMonthDay = byMonthDay;
        this.byMonth = byMonth;
        this.bySetPos = bySetPos;
        this.count = count;
        this.until = until;
        this.strategy = strategy;
        this.timezone = timezone;
    }
}
```

`RecurrenceException`:

```java
@Entity
@Table(name = "recurrence_exceptions",
       uniqueConstraints = @UniqueConstraint(columnNames = {"recurrence_rule_id", "exception_date"}))
public class RecurrenceException extends BaseEntity {
    @Column(name = "recurrence_rule_id", nullable = false) public UUID recurrenceRuleId;
    @Column(name = "exception_date", nullable = false) public LocalDate exceptionDate;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 16) public RecurrenceExceptionReason reason;
    @CreationTimestamp @Column(name = "created_at", updatable = false) public LocalDateTime createdAt;
    protected RecurrenceException() {}
    public RecurrenceException(UUID ruleId, LocalDate date, RecurrenceExceptionReason reason) {
        this.recurrenceRuleId = ruleId;
        this.exceptionDate = date;
        this.reason = reason;
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceEntityTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/model src/test/java/dev/iury/lifeos/task/domain/model/RecurrenceEntityTest.java
rtk git commit -m "feat: map recurrence entities"
```

### Task 6: Entidade Task e seus enums

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/TaskStatus.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/TaskPriority.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/Task.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/model/TaskEntityTest.java`

**Interfaces:**
- Produces: `Task` com defaults `TODO`, `P4`, `America/Sao_Paulo`, `false` e versionamento iniciado em `1`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskEntityTest {
    @Inject EntityManager entityManager;

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
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskEntityTest" test`

Expected: compilation failure porque `Task`, `TaskStatus` e `TaskPriority` não existem.

- [ ] **Step 3: implementar o modelo**

```java
public enum TaskStatus { TODO, IN_PROGRESS, BLOCKED, COMPLETED, CANCELED, OVERDUE }
```

```java
public enum TaskPriority { P1, P2, P3, P4 }
```

```java
@Entity
@Table(name = "tasks")
public class Task extends BaseEntity {
    @Column(nullable = false, length = 255) public String title;
    @Column(columnDefinition = "text") public String description;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 20) public TaskStatus status = TaskStatus.TODO;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 2) public TaskPriority priority = TaskPriority.P4;
    @Column(name = "start_date") public LocalDateTime startDate;
    @Column(name = "due_date") public LocalDateTime dueDate;
    @Column(name = "due_time") public LocalTime dueTime;
    @Column(nullable = false, length = 64) public String timezone = "America/Sao_Paulo";
    @Column(name = "estimated_duration") public Integer estimatedDuration;
    @Column(name = "requires_payment", nullable = false) public boolean requiresPayment;
    @Column(name = "expected_amount", precision = 19, scale = 2) public BigDecimal expectedAmount;
    @Column(name = "completion_event_published", nullable = false) public boolean completionEventPublished;
    @Column(name = "project_id", nullable = false) public UUID projectId;
    @Column(name = "parent_task_id") public UUID parentTaskId;
    @Column(name = "recurrence_rule_id") public UUID recurrenceRuleId;
    @Column(name = "parent_recurring_task_id") public UUID parentRecurringTaskId;
    @Column(name = "is_recurring_master", nullable = false) public boolean recurringMaster;
    @Version @Column(nullable = false) public Integer version = 1;
    @Column(name = "completed_at") public LocalDateTime completedAt;
    @Column(name = "deleted_at") public LocalDateTime deletedAt;
    @CreationTimestamp @Column(name = "created_at", updatable = false) public LocalDateTime createdAt;
    @UpdateTimestamp @Column(name = "updated_at") public LocalDateTime updatedAt;

    protected Task() {}
    public static Task create(String title, UUID projectId) {
        Task task = new Task();
        task.title = title;
        task.projectId = projectId;
        return task;
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskEntityTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/model src/test/java/dev/iury/lifeos/task/domain/model/TaskEntityTest.java
rtk git commit -m "feat: map task entity"
```

### Task 7: Entidades de associação, dependência e auditoria

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/TaskTagId.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/TaskTag.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/DependencyType.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/TaskDependency.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/model/ActivityLog.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/model/TaskRelationEntityTest.java`

**Interfaces:**
- Produces: PK composta `TaskTagId`; aresta direcionada `TaskDependency`; auditoria imutável `ActivityLog`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskRelationEntityTest {
    @Inject EntityManager entityManager;

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
        assertEquals(1L, entityManager.createQuery("select count(t) from TaskTag t", Long.class).getSingleResult());
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationEntityTest" test`

Expected: compilation failure por ausência dos tipos.

- [ ] **Step 3: implementar os tipos**

```java
@Embeddable
public record TaskTagId(@Column(name = "task_id") UUID taskId,
                        @Column(name = "tag_id") UUID tagId) implements Serializable {}
```

```java
@Entity
@Table(name = "task_tags")
public class TaskTag {
    @EmbeddedId public TaskTagId id;
    protected TaskTag() {}
    public TaskTag(UUID taskId, UUID tagId) { this.id = new TaskTagId(taskId, tagId); }
}
```

```java
public enum DependencyType { BLOCKS, RELATES_TO, DUPLICATES }
```

```java
@Entity
@Table(name = "task_dependencies")
public class TaskDependency extends BaseEntity {
    @Column(name = "blocking_task_id", nullable = false) public UUID blockingTaskId;
    @Column(name = "blocked_task_id", nullable = false) public UUID blockedTaskId;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 16) public DependencyType type;
    protected TaskDependency() {}
    public TaskDependency(UUID blockingId, UUID blockedId, DependencyType type) {
        this.blockingTaskId = blockingId;
        this.blockedTaskId = blockedId;
        this.type = type;
    }
}
```

```java
@Entity
@Table(name = "activity_logs")
public class ActivityLog extends BaseEntity {
    @Column(name = "task_id", nullable = false) public UUID taskId;
    @Enumerated(EnumType.STRING) @Column(name = "from_status", length = 20) public TaskStatus fromStatus;
    @Enumerated(EnumType.STRING) @Column(name = "to_status", nullable = false, length = 20) public TaskStatus toStatus;
    @Column(length = 1000) public String reason;
    @Column(name = "occurred_at", nullable = false, updatable = false) public LocalDateTime timestamp;
    protected ActivityLog() {}
    public ActivityLog(UUID taskId, TaskStatus from, TaskStatus to, String reason, LocalDateTime timestamp) {
        this.taskId = taskId;
        this.fromStatus = from;
        this.toStatus = to;
        this.reason = reason;
        this.timestamp = timestamp;
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationEntityTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/model src/test/java/dev/iury/lifeos/task/domain/model/TaskRelationEntityTest.java
rtk git commit -m "feat: map task relations and audit"
```

### Task 8: Repositories de Project e Tag

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/persistence/ProjectRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/persistence/TagRepository.java`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/ProjectTagRepositoryTest.java`

**Interfaces:**
- Produces: `Optional<Project> findInbox()`, `List<Project> listOrdered()`, `Optional<Tag> findByName(String)`, `List<Tag> listOrdered()`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class ProjectTagRepositoryTest {
    @Inject ProjectRepository projects;
    @Inject TagRepository tags;

    @Test
    @TestTransaction
    void findsInboxAndTagByExactName() {
        projects.persist(new Project("Inbox", null, true));
        tags.persist(new Tag("casa", "#123456"));
        assertEquals("Inbox", projects.findInbox().orElseThrow().name);
        assertEquals("casa", tags.findByName("casa").orElseThrow().name);
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectTagRepositoryTest" test`

Expected: compilation failure por ausência dos repositories.

- [ ] **Step 3: implementar os repositories**

```java
@ApplicationScoped
public class ProjectRepository implements PanacheRepositoryBase<Project, UUID> {
    public Optional<Project> findInbox() { return find("inbox", true).firstResultOptional(); }
    public List<Project> listOrdered() { return list(Sort.by("name")); }
}
```

```java
@ApplicationScoped
public class TagRepository implements PanacheRepositoryBase<Tag, UUID> {
    public Optional<Tag> findByName(String name) { return find("name", name).firstResultOptional(); }
    public List<Tag> listOrdered() { return list(Sort.by("name")); }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectTagRepositoryTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence/ProjectRepository.java src/main/java/dev/iury/lifeos/task/persistence/TagRepository.java src/test/java/dev/iury/lifeos/task/persistence/ProjectTagRepositoryTest.java
rtk git commit -m "feat: add project and tag repositories"
```

### Task 9: Repositories de recorrência

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/persistence/RecurrenceRuleRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/persistence/RecurrenceExceptionRepository.java`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/RecurrenceRepositoryTest.java`

**Interfaces:**
- Produces: persistência de regras; `boolean exists(UUID, LocalDate)` para EXDATE.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class RecurrenceRepositoryTest {
    @Inject RecurrenceRuleRepository rules;
    @Inject RecurrenceExceptionRepository exceptions;

    @Test
    @TestTransaction
    void detectsExistingExceptionDate() {
        RecurrenceRule rule = new RecurrenceRule("FREQ=DAILY", RecurrenceFrequency.DAILY, 1,
                null, null, null, null, null, null,
                RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        rules.persist(rule);
        LocalDate date = LocalDate.of(2026, 7, 23);
        exceptions.persist(new RecurrenceException(rule.id, date, RecurrenceExceptionReason.SKIPPED));
        assertTrue(exceptions.exists(rule.id, date));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceRepositoryTest" test`

Expected: compilation failure por ausência dos repositories.

- [ ] **Step 3: implementar os repositories**

```java
@ApplicationScoped
public class RecurrenceRuleRepository implements PanacheRepositoryBase<RecurrenceRule, UUID> {}
```

```java
@ApplicationScoped
public class RecurrenceExceptionRepository implements PanacheRepositoryBase<RecurrenceException, UUID> {
    public boolean exists(UUID ruleId, LocalDate date) {
        return count("recurrenceRuleId = ?1 and exceptionDate = ?2", ruleId, date) > 0;
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceRepositoryTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence/RecurrenceRuleRepository.java src/main/java/dev/iury/lifeos/task/persistence/RecurrenceExceptionRepository.java src/test/java/dev/iury/lifeos/task/persistence/RecurrenceRepositoryTest.java
rtk git commit -m "feat: add recurrence repositories"
```

### Task 10: TaskRepository e filtros combináveis

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskFilter.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/PageResult.java`
- Create: `src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/TaskRepositoryTest.java`

**Interfaces:**
- Produces: `PageResult<Task> findActive(TaskFilter, Instant, int, int)`, `findActiveById`, `listTrash`, `listOverdue`, `listChildren`, `countSeries`, `hardDeleteBefore`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskRepositoryTest {
    @Inject TaskRepository tasks;
    @Inject ProjectRepository projects;

    @Test
    @TestTransaction
    void filtersVisibleActiveTasksAndHidesFutureStart() {
        Project inbox = new Project("Inbox", null, true);
        projects.persist(inbox);
        Task visible = Task.create("visível", inbox.id);
        visible.priority = TaskPriority.P1;
        Task future = Task.create("futura", inbox.id);
        future.priority = TaskPriority.P1;
        future.startDate = LocalDateTime.of(2026, 7, 23, 10, 0);
        tasks.persist(visible, future);
        PageResult<Task> result = tasks.findActive(
                new TaskFilter(null, TaskPriority.P1, null, null, null, null),
                Instant.parse("2026-07-22T13:00:00Z"), 0, 20);
        assertEquals(List.of("visível"), result.items().stream().map(t -> t.title).toList());
        assertEquals(1, result.total());
    }
}
```

Use os records:

```java
public record TaskFilter(TaskStatus status, TaskPriority priority, UUID projectId,
                         UUID tagId, LocalDateTime dueFrom, LocalDateTime dueTo) {}
public record PageResult<T>(List<T> items, long total, int page, int size) {}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRepositoryTest" test`

Expected: compilation failure porque `TaskRepository` ainda não existe.

- [ ] **Step 3: implementar consultas fixas e paginação**

`TaskRepository.findActive` deve montar apenas cláusulas pré-definidas, usar `Parameters`, aplicar `Page.of(page,size)` e ordenar com `Sort.by("dueDate").nullsLast().and("createdAt", Sort.Direction.Descending)`. A consulta base é:

```java
StringBuilder hql = new StringBuilder("deletedAt is null and (startDate is null or startDate <= function('timezone', timezone, :now))");
Parameters parameters = Parameters.with("now", now);
if (filter.status() != null) { hql.append(" and status = :status"); parameters.and("status", filter.status()); }
if (filter.priority() != null) { hql.append(" and priority = :priority"); parameters.and("priority", filter.priority()); }
if (filter.projectId() != null) { hql.append(" and projectId = :projectId"); parameters.and("projectId", filter.projectId()); }
if (filter.dueFrom() != null) { hql.append(" and dueDate >= :dueFrom"); parameters.and("dueFrom", filter.dueFrom()); }
if (filter.dueTo() != null) { hql.append(" and dueDate <= :dueTo"); parameters.and("dueTo", filter.dueTo()); }
if (filter.tagId() != null) {
    hql.append(" and id in (select tt.id.taskId from TaskTag tt where tt.id.tagId = :tagId)");
    parameters.and("tagId", filter.tagId());
}
PanacheQuery<Task> query = find(hql.toString(),
        Sort.by("dueDate").nullsLast().and("createdAt", Sort.Direction.Descending), parameters);
long total = query.count();
List<Task> items = query.page(Page.of(page, size)).list();
return new PageResult<>(items, total, page, size);
```

Implemente também:

```java
public Optional<Task> findActiveById(UUID id) { return find("id = ?1 and deletedAt is null", id).firstResultOptional(); }
public List<Task> listTrash() { return list("deletedAt is not null", Sort.by("deletedAt", Sort.Direction.Descending)); }
public List<Task> listOverdue() { return list("deletedAt is null and status", TaskStatus.OVERDUE); }
public List<Task> listChildren(UUID parentId) { return list("parentTaskId = ?1 and deletedAt is null", parentId); }
public long countSeries(UUID rootId) { return count("id = ?1 or parentRecurringTaskId = ?1", rootId); }
public long hardDeleteBefore(Instant cutoff) {
    return delete("deletedAt is not null and deletedAt < function('timezone', timezone, ?1)", cutoff);
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRepositoryTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TaskFilter.java src/main/java/dev/iury/lifeos/task/application/PageResult.java src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java src/test/java/dev/iury/lifeos/task/persistence/TaskRepositoryTest.java
rtk git commit -m "feat: add filtered task repository"
```

### Task 11: Repositories de relações e auditoria

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/persistence/TaskTagRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/persistence/TaskDependencyRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/persistence/ActivityLogRepository.java`
- Test: `src/test/java/dev/iury/lifeos/task/persistence/TaskRelationRepositoryTest.java`

**Interfaces:**
- Produces: contagem/lista de tags; arestas de entrada/saída; histórico cronológico.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskRelationRepositoryTest {
    @Inject TaskDependencyRepository dependencies;
    @Inject ActivityLogRepository activity;
    @Inject EntityManager entityManager;

    @Test
    @TestTransaction
    void listsPendingBlockersAndHistoryInOrder() {
        Project project = new Project("Inbox", null, true);
        entityManager.persist(project);
        Task first = Task.create("A", project.id);
        Task second = Task.create("B", project.id);
        entityManager.persist(first);
        entityManager.persist(second);
        dependencies.persist(new TaskDependency(first.id, second.id, DependencyType.BLOCKS));
        activity.persist(new ActivityLog(second.id, null, TaskStatus.TODO, null,
                LocalDateTime.of(2026, 7, 22, 9, 0)));
        assertEquals(1, dependencies.listBlocking(second.id).size());
        assertEquals(TaskStatus.TODO, activity.listByTask(second.id).getFirst().toStatus);
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationRepositoryTest" test`

Expected: compilation failure por ausência dos repositories.

- [ ] **Step 3: implementar consultas**

```java
@ApplicationScoped
public class TaskTagRepository implements PanacheRepositoryBase<TaskTag, TaskTagId> {
    public long countByTask(UUID taskId) { return count("id.taskId", taskId); }
    public List<TaskTag> listByTask(UUID taskId) { return list("id.taskId", taskId); }
    public long deleteByTag(UUID tagId) { return delete("id.tagId", tagId); }
}
```

```java
@ApplicationScoped
public class TaskDependencyRepository implements PanacheRepositoryBase<TaskDependency, UUID> {
    public List<TaskDependency> listBlocking(UUID blockedId) {
        return list("blockedTaskId = ?1 and type = ?2", blockedId, DependencyType.BLOCKS);
    }
    public List<TaskDependency> listOutgoingBlocks(UUID blockingId) {
        return list("blockingTaskId = ?1 and type = ?2", blockingId, DependencyType.BLOCKS);
    }
    public List<TaskDependency> listAllForTask(UUID taskId) {
        return list("blockingTaskId = ?1 or blockedTaskId = ?1", taskId);
    }
    public boolean exists(UUID blockingId, UUID blockedId) {
        return count("blockingTaskId = ?1 and blockedTaskId = ?2", blockingId, blockedId) > 0;
    }
}
```

```java
@ApplicationScoped
public class ActivityLogRepository implements PanacheRepositoryBase<ActivityLog, UUID> {
    public List<ActivityLog> listByTask(UUID taskId) {
        return list("taskId", Sort.by("timestamp"), taskId);
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationRepositoryTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence src/test/java/dev/iury/lifeos/task/persistence/TaskRelationRepositoryTest.java
rtk git commit -m "feat: add task relation repositories"
```

### Task 12: Validação básica e sanitização de Task

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/DomainException.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/ValidationException.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/validation/TaskInputValidator.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/validation/TaskInputValidatorTest.java`

**Interfaces:**
- Produces: `normalizeTitle`, `sanitizeDescription`, `validateDates`, `validateTimezone`, `validateDuration`, `validatePayment`.

- [ ] **Step 1: escrever testes falhos para cada regra**

```java
class TaskInputValidatorTest {
    private final TaskInputValidator validator = new TaskInputValidator();

    @Test void trimsTitle() { assertEquals("Pagar luz", validator.normalizeTitle("  Pagar luz  ")); }
    @Test void rejectsBlankTitle() { assertThrows(ValidationException.class, () -> validator.normalizeTitle("  ")); }
    @Test void rejectsLongTitle() { assertThrows(ValidationException.class, () -> validator.normalizeTitle("x".repeat(256))); }
    @Test void stripsHtmlFromDescription() { assertEquals("texto", validator.sanitizeDescription("<script>alert(1)</script><b>texto</b>")); }
    @Test void rejectsDescriptionOver64KiB() { assertThrows(ValidationException.class, () -> validator.sanitizeDescription("ç".repeat(32769))); }
    @Test void rejectsStartAfterDue() { assertThrows(ValidationException.class, () -> validator.validateDates(LocalDateTime.of(2026,7,23,0,0), LocalDateTime.of(2026,7,22,0,0))); }
    @Test void rejectsInvalidTimezone() { assertThrows(ValidationException.class, () -> validator.validateTimezone("Mars/Olympus")); }
    @Test void rejectsZeroDuration() { assertThrows(ValidationException.class, () -> validator.validateDuration(0)); }
    @Test void rejectsAmountWithoutPayment() { assertThrows(ValidationException.class, () -> validator.validatePayment(false, BigDecimal.TEN)); }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskInputValidatorTest" test`

Expected: compilation failure por ausência dos tipos.

- [ ] **Step 3: implementar o mínimo**

```java
public abstract class DomainException extends RuntimeException {
    private final String error;
    private final int status;
    protected DomainException(String error, String message, int status) {
        super(message);
        this.error = error;
        this.status = status;
    }
    public String error() { return error; }
    public int status() { return status; }
}
```

```java
public final class ValidationException extends DomainException {
    public ValidationException(String message) { super("VALIDATION_ERROR", message, 400); }
}
```

```java
@ApplicationScoped
public class TaskInputValidator {
    public String normalizeTitle(String title) {
        if (title == null || title.trim().isEmpty()) throw new ValidationException("title deve ter entre 1 e 255 caracteres");
        String normalized = title.trim();
        if (normalized.length() > 255) throw new ValidationException("title deve ter entre 1 e 255 caracteres");
        return normalized;
    }
    public String sanitizeDescription(String description) {
        if (description == null) return null;
        if (description.getBytes(StandardCharsets.UTF_8).length > 65_536) throw new ValidationException("description deve ter no máximo 64KB");
        return Jsoup.clean(description, Safelist.none());
    }
    public void validateDates(LocalDateTime start, LocalDateTime due) {
        if (start != null && due != null && start.isAfter(due)) throw new ValidationException("startDate deve ser menor ou igual a dueDate");
    }
    public String validateTimezone(String timezone) {
        String value = timezone == null ? "America/Sao_Paulo" : timezone;
        try { ZoneId.of(value); return value; }
        catch (DateTimeException exception) { throw new ValidationException("timezone IANA inválido: " + value); }
    }
    public void validateDuration(Integer minutes) {
        if (minutes != null && (minutes < 1 || minutes > 525_600)) throw new ValidationException("estimatedDuration deve estar entre 1 e 525600");
    }
    public void validatePayment(boolean required, BigDecimal amount) {
        if (!required && amount != null) throw new ValidationException("expectedAmount exige requiresPayment=true");
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskInputValidatorTest" test`

Expected: PASS, 9 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error src/main/java/dev/iury/lifeos/task/domain/validation src/test/java/dev/iury/lifeos/task/domain/validation/TaskInputValidatorTest.java
rtk git commit -m "feat: validate and sanitize task input"
```

### Task 13: ProjectService e Inbox único

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/ProjectNotFoundException.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/ProjectDetails.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/ProjectService.java`
- Modify: `src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/ProjectServiceTest.java`

**Interfaces:**
- Produces: `create`, `inbox`, `list`, `get`, `update`; `ProjectDetails(Project project,long taskCount)`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class ProjectServiceTest {
    @Inject ProjectService service;

    @Test
    @TestTransaction
    void createsInboxLazilyAndReusesIt() {
        Project first = service.inbox();
        Project second = service.inbox();
        assertEquals(first.id, second.id);
        assertTrue(first.inbox);
    }

    @Test
    @TestTransaction
    void trimsProjectAndReportsTaskCount() {
        Project project = service.create("  Casa  ", "tarefas domésticas");
        assertEquals("Casa", project.name);
        assertEquals(0, service.get(project.id).taskCount());
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectServiceTest" test`

Expected: compilation failure por ausência do serviço.

- [ ] **Step 3: implementar ProjectService**

Adicione a `TaskRepository`:

```java
public long countActiveByProject(UUID projectId) { return count("projectId = ?1 and deletedAt is null", projectId); }
```

Use:

```java
public record ProjectDetails(Project project, long taskCount) {}
```

```java
public final class ProjectNotFoundException extends DomainException {
    public ProjectNotFoundException(UUID id) { super("NOT_FOUND", "Projeto com id '" + id + "' não encontrado", 404); }
}
```

`ProjectService` deve ser `@ApplicationScoped`; métodos de escrita usam `@Transactional`:

```java
public Project inbox() {
    return projects.findInbox().orElseGet(() -> {
        Project inbox = new Project("Inbox", null, true);
        projects.persist(inbox);
        return inbox;
    });
}
public Project create(String name, String description) {
    String normalized = normalizeName(name);
    Project project = new Project(normalized, description, false);
    projects.persist(project);
    return project;
}
public List<Project> list() { return projects.listOrdered(); }
public ProjectDetails get(UUID id) {
    Project project = projects.findByIdOptional(id).orElseThrow(() -> new ProjectNotFoundException(id));
    return new ProjectDetails(project, tasks.countActiveByProject(id));
}
public Project update(UUID id, String name, String description) {
    Project project = get(id).project();
    String normalized = normalizeName(name);
    if (project.inbox && !"Inbox".equals(normalized)) throw new ValidationException("O projeto Inbox não pode ser renomeado");
    project.name = normalized;
    project.description = description;
    return project;
}
private String normalizeName(String name) {
    if (name == null || name.trim().isEmpty() || name.trim().length() > 100) throw new ValidationException("name deve ter entre 1 e 100 caracteres");
    return name.trim();
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/Project* src/main/java/dev/iury/lifeos/task/domain/error/ProjectNotFoundException.java src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java src/test/java/dev/iury/lifeos/task/application/ProjectServiceTest.java
rtk git commit -m "feat: manage projects and inbox"
```

### Task 14: TagService

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/TagNotFoundException.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TagService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TagServiceTest.java`

**Interfaces:**
- Produces: `create(String,String)`, `list()`, `get(UUID)`, `delete(UUID)`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TagServiceTest {
    @Inject TagService service;

    @Test
    @TestTransaction
    void createsNormalizedUniqueTag() {
        Tag tag = service.create("  urgente  ", "#AABBCC");
        assertEquals("urgente", tag.name);
        assertThrows(ValidationException.class, () -> service.create("urgente", "#AABBCC"));
    }

    @Test
    @TestTransaction
    void rejectsInvalidColor() {
        assertThrows(ValidationException.class, () -> service.create("x", "red"));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TagServiceTest" test`

Expected: compilation failure por ausência do serviço.

- [ ] **Step 3: implementar TagService**

```java
public final class TagNotFoundException extends DomainException {
    public TagNotFoundException(UUID id) { super("NOT_FOUND", "Tag com id '" + id + "' não encontrada", 404); }
}
```

```java
@ApplicationScoped
public class TagService {
    @Inject TagRepository tags;
    @Inject TaskTagRepository taskTags;

    @Transactional
    public Tag create(String name, String color) {
        if (name == null || name.trim().isEmpty() || name.trim().length() > 50) throw new ValidationException("name deve ter entre 1 e 50 caracteres");
        String normalized = name.trim();
        if (color != null && !color.matches("^#[0-9A-Fa-f]{6}$")) throw new ValidationException("color deve usar o formato #RRGGBB");
        if (tags.findByName(normalized).isPresent()) throw new ValidationException("Já existe uma tag com o nome '" + normalized + "'");
        Tag tag = new Tag(normalized, color);
        tags.persist(tag);
        return tag;
    }
    public List<Tag> list() { return tags.listOrdered(); }
    public Tag get(UUID id) { return tags.findByIdOptional(id).orElseThrow(() -> new TagNotFoundException(id)); }
    @Transactional public void delete(UUID id) {
        get(id);
        taskTags.deleteByTag(id);
        tags.deleteById(id);
    }
}
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TagServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TagService.java src/main/java/dev/iury/lifeos/task/domain/error/TagNotFoundException.java src/test/java/dev/iury/lifeos/task/application/TagServiceTest.java
rtk git commit -m "feat: manage task tags"
```

### Task 15: Parser e persistência de RRULE

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/InvalidRecurrenceRuleException.java`
- Create: `src/main/java/dev/iury/lifeos/task/recurrence/RecurrenceInput.java`
- Create: `src/main/java/dev/iury/lifeos/task/recurrence/RecurrenceRuleService.java`
- Test: `src/test/java/dev/iury/lifeos/task/recurrence/RecurrenceRuleServiceTest.java`

**Interfaces:**
- Consumes: `RecurrenceInput(String rruleString, RecurrenceStrategy strategy, String timezone)`.
- Produces: regra canônica validada e persistida; campos derivados da string, sem duplicar entrada da API.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class RecurrenceRuleServiceTest {
    @Inject RecurrenceRuleService service;

    @Test
    @TestTransaction
    void parsesAndPersistsCanonicalWeeklyRule() {
        RecurrenceRule rule = service.create(new RecurrenceInput(
                "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU,TH", RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo"));
        assertEquals(RecurrenceFrequency.WEEKLY, rule.freq);
        assertEquals(2, rule.interval);
        assertEquals("TU,TH", rule.byDay);
    }

    @Test void rejectsCountAndUntilTogether() {
        assertThrows(InvalidRecurrenceRuleException.class, () -> service.create(new RecurrenceInput(
                "FREQ=DAILY;COUNT=2;UNTIL=20260730T000000", RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo")));
    }

    @Test void rejectsInvalidTimezone() {
        assertThrows(InvalidRecurrenceRuleException.class, () -> service.create(
                new RecurrenceInput("FREQ=DAILY", RecurrenceStrategy.FIXED_SCHEDULE, "Invalid/Zone")));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceRuleServiceTest" test`

Expected: compilation failure por ausência dos tipos.

- [ ] **Step 3: implementar parsing com ical4j**

```java
public record RecurrenceInput(String rruleString, RecurrenceStrategy strategy, String timezone) {}
```

```java
public final class InvalidRecurrenceRuleException extends DomainException {
    public InvalidRecurrenceRuleException(String message) { super("INVALID_RECURRENCE_RULE", message, 400); }
}
```

`RecurrenceRuleService.create` deve:

```java
@Transactional
public RecurrenceRule create(RecurrenceInput input) {
    try {
        ZoneId.of(input.timezone());
        Recur<LocalDateTime> parsed = new Recur<>(input.rruleString());
        if (parsed.getCount() > 0 && parsed.getUntil() != null) throw new InvalidRecurrenceRuleException("count e until são mutuamente exclusivos");
        requireAtMostOne(parsed.getMonthDayList(), "BYMONTHDAY");
        requireAtMostOne(parsed.getMonthList(), "BYMONTH");
        requireAtMostOne(parsed.getSetPosList(), "BYSETPOS");
        RecurrenceRule rule = new RecurrenceRule(
                parsed.toString(), RecurrenceFrequency.valueOf(parsed.getFrequency().name()),
                parsed.getInterval() < 1 ? 1 : parsed.getInterval(), joinDays(parsed.getDayList()),
                firstInteger(parsed.getMonthDayList()), firstMonth(parsed.getMonthList()),
                firstInteger(parsed.getSetPosList()), parsed.getCount() > 0 ? parsed.getCount() : null,
                parsed.getUntil(), input.strategy(), input.timezone());
        rules.persist(rule);
        return rule;
    } catch (InvalidRecurrenceRuleException exception) {
        throw exception;
    } catch (RuntimeException exception) {
        throw new InvalidRecurrenceRuleException("RRULE inválida segundo RFC 5545: " + input.rruleString());
    }
}
```

Use estes helpers; antes do `try`, rejeite `input==null`, strategy nula, RRULE nula/vazia e timezone nulo/vazio com `InvalidRecurrenceRuleException`:

```java
private void requireAtMostOne(List<?> values, String field) {
    if (values.size() > 1) throw new InvalidRecurrenceRuleException(field + " aceita um valor neste modelo");
}
private String joinDays(List<WeekDay> values) {
    return values.isEmpty() ? null : values.stream().map(WeekDay::toString).collect(Collectors.joining(","));
}
private Integer firstInteger(List<Integer> values) {
    return values.isEmpty() ? null : values.getFirst();
}
private Integer firstMonth(List<Month> values) {
    return values.isEmpty() ? null : values.getFirst().getMonthOfYear();
}
```

Depois de construir `parsed`, rejeite frequências diferentes de DAILY/WEEKLY/MONTHLY/YEARLY antes de persistir.

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceRuleServiceTest" test`

Expected: PASS, 3 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/recurrence src/main/java/dev/iury/lifeos/task/domain/error/InvalidRecurrenceRuleException.java src/test/java/dev/iury/lifeos/task/recurrence/RecurrenceRuleServiceTest.java
rtk git commit -m "feat: parse and persist recurrence rules"
```

### Task 16: Criação de tarefa comum ou recorrente

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/application/CreateTaskCommand.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/ClockProducer.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskCreationService.java`
- Create: `src/test/java/dev/iury/lifeos/task/support/FixedClockProducer.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskCreationServiceTest.java`

**Interfaces:**
- Produces: `Task create(CreateTaskCommand)`; cria Inbox quando `projectId` é nulo; cria RRULE quando `recurrence` existe.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskCreationServiceTest {
    @Inject TaskCreationService service;

    @Test
    @TestTransaction
    void createsSanitizedTaskInInbox() {
        Task task = service.create(command("  Pagar luz  ", "<b>Conta</b>", null, null, false, null));
        assertEquals("Pagar luz", task.title);
        assertEquals("Conta", task.description);
        assertNotNull(task.projectId);
        assertEquals(TaskStatus.TODO, task.status);
    }

    @Test void requiresDueDateForRecurrence() {
        RecurrenceInput recurrence = new RecurrenceInput("FREQ=DAILY", RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        assertThrows(ValidationException.class, () -> service.create(command("x", null, null, recurrence, false, null)));
    }

    private CreateTaskCommand command(String title, String description, LocalDateTime dueDate,
            RecurrenceInput recurrence, boolean requiresPayment, BigDecimal expectedAmount) {
        return new CreateTaskCommand(title, description, TaskPriority.P4, null, dueDate, null,
                "America/Sao_Paulo", null, requiresPayment, expectedAmount, null, null, recurrence);
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskCreationServiceTest" test`

Expected: compilation failure por ausência do command/service.

- [ ] **Step 3: implementar a criação transacional**

```java
public record CreateTaskCommand(
        String title, String description, TaskPriority priority,
        LocalDateTime startDate, LocalDateTime dueDate, LocalTime dueTime,
        String timezone, Integer estimatedDuration, boolean requiresPayment,
        BigDecimal expectedAmount, UUID projectId, UUID parentTaskId,
        RecurrenceInput recurrence) {}
```

`TaskCreationService.create`:

```java
@Transactional
public Task create(CreateTaskCommand command) {
    validator.validateDates(command.startDate(), command.dueDate());
    validator.validateDuration(command.estimatedDuration());
    validator.validatePayment(command.requiresPayment(), command.expectedAmount());
    if (command.recurrence() != null && command.dueDate() == null) throw new ValidationException("dueDate é obrigatório para tarefa recorrente");
    Project project = command.projectId() == null ? projects.inbox() : projects.get(command.projectId()).project();
    Task task = Task.create(validator.normalizeTitle(command.title()), project.id);
    task.description = validator.sanitizeDescription(command.description());
    task.priority = command.priority() == null ? TaskPriority.P4 : command.priority();
    task.startDate = command.startDate();
    task.dueDate = command.dueDate();
    task.dueTime = command.dueTime();
    task.timezone = validator.validateTimezone(command.timezone());
    task.estimatedDuration = command.estimatedDuration();
    task.requiresPayment = command.requiresPayment();
    task.expectedAmount = command.expectedAmount();
    task.parentTaskId = command.parentTaskId();
    if (command.recurrence() != null) {
        RecurrenceRule rule = recurrenceRules.create(command.recurrence());
        task.recurrenceRuleId = rule.id;
        task.recurringMaster = true;
    }
    tasks.persist(task);
    activity.persist(new ActivityLog(task.id, null, TaskStatus.TODO, null, now(task.timezone)));
    return task;
}
```

Injete repositories/services/validator e este producer:

```java
@ApplicationScoped
public class ClockProducer {
    @Produces @Singleton
    Clock clock() { return Clock.systemUTC(); }
}
```

Para todos os testes Quarkus, crie a alternativa determinística:

```java
@Alternative
@Priority(1)
@Singleton
public class FixedClockProducer {
    @Produces
    Clock clock() { return Clock.fixed(Instant.parse("2026-07-22T13:00:00Z"), ZoneOffset.UTC); }
}
```

`now(zone)` usa `LocalDateTime.ofInstant(clock.instant(), ZoneId.of(zone))`.

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskCreationServiceTest" test`

Expected: PASS, 2 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/CreateTaskCommand.java src/main/java/dev/iury/lifeos/task/application/TaskCreationService.java src/main/java/dev/iury/lifeos/task/application/ClockProducer.java src/test/java/dev/iury/lifeos/task/application/TaskCreationServiceTest.java src/test/java/dev/iury/lifeos/task/support/FixedClockProducer.java
rtk git commit -m "feat: create tasks and recurrence"
```

### Task 17: Regras de subtarefa

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/TaskNotFoundException.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/MaxSubtaskDepthException.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/validation/SubtaskValidator.java`
- Create: `src/test/java/dev/iury/lifeos/task/support/PersistenceTestData.java`
- Modify: `src/main/java/dev/iury/lifeos/task/application/TaskCreationService.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/validation/SubtaskValidatorTest.java`

**Interfaces:**
- Produces: `Task requireActive(UUID)`, `void validateParent(Task,LocalDateTime)`, herança obrigatória de `projectId`.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class SubtaskValidatorTest {
    @Inject SubtaskValidator validator;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void rejectsDueDateAfterParent() {
        Task parent = data.taskWithDue(LocalDateTime.of(2026, 7, 25, 10, 0));
        assertThrows(ValidationException.class, () -> validator.validateParent(parent,
                LocalDateTime.of(2026, 7, 26, 10, 0)));
    }

    @Test
    @TestTransaction
    void rejectsSixthLevel() {
        Task level5 = data.levelFiveParent();
        assertThrows(MaxSubtaskDepthException.class, () -> validator.validateParent(level5, null));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=SubtaskValidatorTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar profundidade e herança**

```java
public final class TaskNotFoundException extends DomainException {
    public TaskNotFoundException(UUID id) { super("NOT_FOUND", "Tarefa com id '" + id + "' não encontrada", 404); }
}
public final class MaxSubtaskDepthException extends DomainException {
    public MaxSubtaskDepthException() { super("MAX_SUBTASK_DEPTH", "Subtarefa excede 5 níveis de profundidade", 400); }
}
```

```java
@ApplicationScoped
public class SubtaskValidator {
    @Inject TaskRepository tasks;
    public Task requireActive(UUID id) { return tasks.findActiveById(id).orElseThrow(() -> new TaskNotFoundException(id)); }
    public void validateParent(Task parent, LocalDateTime childDueDate) {
        int depth = 1;
        Task cursor = parent;
        while (cursor.parentTaskId != null) {
            depth++;
            if (depth >= 5) throw new MaxSubtaskDepthException();
            cursor = requireActive(cursor.parentTaskId);
        }
        if (parent.dueDate != null && childDueDate != null && childDueDate.isAfter(parent.dueDate))
            throw new ValidationException("dueDate da subtarefa não pode ultrapassar dueDate da tarefa pai");
    }
}
```

Em `TaskCreationService`, quando `parentTaskId != null`, carregue e valide o pai e substitua o projeto do command por `projects.get(parent.projectId).project()` antes de preencher `task.parentTaskId`.

Crie o suporte comum que os testes seguintes usarão:

```java
package dev.iury.lifeos.task.support;

import dev.iury.lifeos.task.domain.model.*;
import dev.iury.lifeos.task.persistence.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class PersistenceTestData {
    @Inject ProjectRepository projects;
    @Inject TaskRepository tasks;
    @Inject TagRepository tags;
    @Inject TaskTagRepository taskTags;
    @Inject TaskDependencyRepository dependencies;
    @Inject RecurrenceRuleRepository rules;

    public Project inbox() {
        return projects.findInbox().orElseGet(() -> {
            Project project = new Project("Inbox", null, true);
            projects.persist(project);
            return project;
        });
    }

    public Task task() { return task(TaskStatus.TODO); }

    public Task task(TaskStatus status) {
        Task task = Task.create("Tarefa " + tasks.count(), inbox().id);
        task.status = status;
        tasks.persist(task);
        return task;
    }

    public List<Task> tasks(int count) {
        List<Task> result = new ArrayList<>();
        for (int index = 0; index < count; index++) result.add(task());
        return result;
    }

    public Task taskWithDue(LocalDateTime dueDate) {
        Task task = task();
        task.dueDate = dueDate;
        return task;
    }

    public Task levelFiveParent() {
        Task parent = task();
        for (int level = 2; level <= 5; level++) {
            Task child = Task.create("Nível " + level, parent.projectId);
            child.parentTaskId = parent.id;
            tasks.persist(child);
            parent = child;
        }
        return parent;
    }

    public Graph graph() {
        Task parent = task();
        Task child = Task.create("Filha", parent.projectId);
        child.parentTaskId = parent.id;
        tasks.persist(child);
        Tag tag = new Tag("casa", "#123456");
        tags.persist(tag);
        taskTags.persist(new TaskTag(parent.id, tag.id));
        TaskDependency dependency = new TaskDependency(parent.id, child.id, DependencyType.RELATES_TO);
        dependencies.persist(dependency);
        return new Graph(parent, child, tag, dependency);
    }

    public Task recurring(TaskStatus status, Integer count, boolean withTag, LocalDateTime dueDate) {
        RecurrenceRule rule = new RecurrenceRule("FREQ=DAILY", RecurrenceFrequency.DAILY, 1,
                null, null, null, null, count, null,
                RecurrenceStrategy.FIXED_SCHEDULE, "America/Sao_Paulo");
        rules.persist(rule);
        Task task = Task.create("Recorrente", inbox().id);
        task.status = status;
        task.dueDate = dueDate;
        task.recurrenceRuleId = rule.id;
        task.recurringMaster = true;
        tasks.persist(task);
        if (withTag) {
            Tag tag = new Tag("recorrente", "#123456");
            tags.persist(tag);
            taskTags.persist(new TaskTag(task.id, tag.id));
        }
        return task;
    }

    public Task inProgressWithOpenChild() {
        Task parent = task(TaskStatus.IN_PROGRESS);
        Task child = Task.create("Aberta", parent.projectId);
        child.parentTaskId = parent.id;
        tasks.persist(child);
        return parent;
    }

    public Task completed(boolean payment) {
        Task task = task(TaskStatus.COMPLETED);
        task.requiresPayment = payment;
        task.expectedAmount = payment ? new BigDecimal("150.00") : null;
        task.completionEventPublished = payment;
        task.completedAt = LocalDateTime.of(2026, 7, 22, 9, 0);
        return task;
    }

    public TrashTree trashTree() {
        Task parent = task();
        Task active = Task.create("Ativa", parent.projectId);
        active.parentTaskId = parent.id;
        tasks.persist(active);
        Task previous = Task.create("Apagada antes", parent.projectId);
        previous.parentTaskId = parent.id;
        previous.deletedAt = LocalDateTime.of(2026, 7, 1, 9, 0);
        tasks.persist(previous);
        return new TrashTree(parent, active, previous);
    }

    public TaggedData tagged(int existingTags) {
        Task task = task();
        List<Tag> created = new ArrayList<>();
        for (int index = 0; index < existingTags; index++) {
            Tag tag = new Tag("tag-" + index, "#123456");
            tags.persist(tag);
            taskTags.persist(new TaskTag(task.id, tag.id));
            created.add(tag);
        }
        return new TaggedData(task, created);
    }

    public Tag tag(String name) {
        Tag tag = new Tag(name, "#123456");
        tags.persist(tag);
        return tag;
    }

    public DueSet dueSet() {
        Task todoPast = task(TaskStatus.TODO);
        todoPast.dueDate = LocalDateTime.of(2026, 7, 21, 9, 0);
        Task progressPast = task(TaskStatus.IN_PROGRESS);
        progressPast.dueDate = LocalDateTime.of(2026, 7, 21, 10, 0);
        Task todoFuture = task(TaskStatus.TODO);
        todoFuture.dueDate = LocalDateTime.of(2026, 7, 23, 9, 0);
        return new DueSet(todoPast, progressPast, todoFuture);
    }

    public TrashSet trashSet() {
        Task old = task();
        old.deletedAt = LocalDateTime.of(2026, 6, 1, 3, 0);
        Task recent = task();
        recent.deletedAt = LocalDateTime.of(2026, 7, 10, 3, 0);
        return new TrashSet(old, recent);
    }

    public Task scalarTask() {
        Task task = task(TaskStatus.COMPLETED);
        task.description = "Descrição";
        task.priority = TaskPriority.P1;
        task.version = 3;
        task.completedAt = LocalDateTime.of(2026, 7, 22, 9, 0);
        return task;
    }

    public Task inProgressPaymentTask(BigDecimal amount) {
        Task task = task(TaskStatus.IN_PROGRESS);
        task.requiresPayment = true;
        task.expectedAmount = amount;
        return task;
    }

    public record Graph(Task parent, Task child, Tag tag, TaskDependency dependency) {}
    public record TrashTree(Task parent, Task activeChild, Task previouslyDeletedChild) {}
    public record TaggedData(Task task, List<Tag> tags) {}
    public record DueSet(Task todoPast, Task progressPast, Task todoFuture) {}
    public record TrashSet(Task old, Task recent) {}
}
```

- [ ] **Step 4: rodar testes focados**

Run: `rtk .\mvnw.cmd "-Dtest=SubtaskValidatorTest,TaskCreationServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error src/main/java/dev/iury/lifeos/task/domain/validation/SubtaskValidator.java src/main/java/dev/iury/lifeos/task/application/TaskCreationService.java src/test/java/dev/iury/lifeos/task/domain/validation/SubtaskValidatorTest.java src/test/java/dev/iury/lifeos/task/support/PersistenceTestData.java
rtk git commit -m "feat: enforce subtask constraints"
```

### Task 18: Consulta, filtros e detalhe de Task

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskDetails.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskQueryService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskQueryServiceTest.java`

**Interfaces:**
- Produces: `get(UUID)`, `list(TaskFilter,int,int)`, `details(UUID)`, `trash()`, `overdue()`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskQueryServiceTest {
    @Inject TaskQueryService service;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void detailIncludesChildrenTagsAndDependencies() {
        PersistenceTestData.Graph graph = data.graph();
        TaskDetails details = service.details(graph.parent().id);
        assertEquals(1, details.subtasks().size());
        assertEquals(1, details.tags().size());
        assertEquals(1, details.dependencies().size());
    }

    @Test void rejectsPageSizeAboveOneHundred() {
        assertThrows(ValidationException.class, () -> service.list(new TaskFilter(null,null,null,null,null,null), 0, 101));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskQueryServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar o serviço de leitura**

```java
public record TaskDetails(Task task, List<Task> subtasks, List<Tag> tags,
                          List<TaskDependency> dependencies) {}
```

`TaskQueryService` valida `page >= 0` e `1 <= size <= 100`; passa `clock.instant()` ao repository, que converte o instante para o timezone de cada linha. Para resolver tags, carregue os ids de `TaskTagRepository.listByTask`, chame `TagRepository.findById` e não descarte ids (a FK garante existência). Métodos:

```java
public Task get(UUID id) { return tasks.findActiveById(id).orElseThrow(() -> new TaskNotFoundException(id)); }
public PageResult<Task> list(TaskFilter filter, int page, int size) { validatePage(page, size); return tasks.findActive(filter, clock.instant(), page, size); }
public TaskDetails details(UUID id) {
    Task task = get(id);
    List<Tag> tags = taskTags.listByTask(id).stream().map(link -> tagRepository.findById(link.id.tagId())).toList();
    return new TaskDetails(task, tasks.listChildren(id), tags, dependencies.listAllForTask(id));
}
public List<Task> trash() { return tasks.listTrash(); }
public List<Task> overdue() { return tasks.listOverdue(); }
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskQueryServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TaskDetails.java src/main/java/dev/iury/lifeos/task/application/TaskQueryService.java src/test/java/dev/iury/lifeos/task/application/TaskQueryServiceTest.java
rtk git commit -m "feat: query task views"
```

### Task 19: Atualização com optimistic locking

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/TaskOptimisticLockException.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/UpdateTaskCommand.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskUpdateService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskUpdateServiceTest.java`

**Interfaces:**
- Produces: `Task update(UUID, UpdateTaskCommand)` exigindo a versão lida pelo cliente.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskUpdateServiceTest {
    @Inject TaskUpdateService service;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void updatesWhenVersionMatches() {
        Task task = data.task();
        Task updated = service.update(task.id, command(task.version, "novo título"));
        assertEquals("novo título", updated.title);
    }

    @Test
    @TestTransaction
    void returnsConflictWhenVersionIsStale() {
        Task task = data.task();
        assertThrows(TaskOptimisticLockException.class, () -> service.update(task.id, command(task.version - 1, "x")));
    }

    private UpdateTaskCommand command(Integer version, String title) {
        return new UpdateTaskCommand(version, title, null, TaskPriority.P4,
                null, null, null, "America/Sao_Paulo", null, false, null);
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskUpdateServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar comparação e atualização**

```java
public record UpdateTaskCommand(Integer version, String title, String description,
        TaskPriority priority, LocalDateTime startDate, LocalDateTime dueDate,
        LocalTime dueTime, String timezone, Integer estimatedDuration,
        boolean requiresPayment, BigDecimal expectedAmount) {}
```

```java
public final class TaskOptimisticLockException extends DomainException {
    public TaskOptimisticLockException(UUID id) { super("OPTIMISTIC_LOCK", "A tarefa '" + id + "' foi alterada por outro processo", 409); }
}
```

`TaskUpdateService.update` carrega ativa, exige `command.version()!=null && equals(task.version)`, mantém project/parent/recurrence/status inalterados, chama `entityManager.flush()` e converte `jakarta.persistence.OptimisticLockException` em `TaskOptimisticLockException`.

```java
if (command.version() == null || !command.version().equals(task.version)) throw new TaskOptimisticLockException(id);
validator.validateDates(command.startDate(), command.dueDate());
validator.validateDuration(command.estimatedDuration());
validator.validatePayment(command.requiresPayment(), command.expectedAmount());
if (task.recurrenceRuleId != null && command.dueDate() == null)
    throw new ValidationException("dueDate é obrigatório para tarefa recorrente");
if (task.parentTaskId != null)
    subtasks.validateParent(subtasks.requireActive(task.parentTaskId), command.dueDate());
task.title = validator.normalizeTitle(command.title());
task.description = validator.sanitizeDescription(command.description());
task.priority = command.priority() == null ? TaskPriority.P4 : command.priority();
task.startDate = command.startDate();
task.dueDate = command.dueDate();
task.dueTime = command.dueTime();
task.timezone = validator.validateTimezone(command.timezone());
task.estimatedDuration = command.estimatedDuration();
task.requiresPayment = command.requiresPayment();
task.expectedAmount = command.expectedAmount();
```

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskUpdateServiceTest" test`

Expected: PASS, incluindo status HTTP lógico 409 na exceção.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error/TaskOptimisticLockException.java src/main/java/dev/iury/lifeos/task/application/UpdateTaskCommand.java src/main/java/dev/iury/lifeos/task/application/TaskUpdateService.java src/test/java/dev/iury/lifeos/task/application/TaskUpdateServiceTest.java
rtk git commit -m "feat: update tasks with optimistic locking"
```

### Task 20: Dependências e detecção de ciclo DAG

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/CircularDependencyException.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskDependencyService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskDependencyServiceTest.java`

**Interfaces:**
- Produces: `create(UUID blockedId, UUID blockingId, DependencyType)`, `list(UUID)`, `delete(UUID,UUID)`.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskDependencyServiceTest {
    @Inject TaskDependencyService service;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void rejectsSelfDependency() {
        Task task = data.task();
        assertThrows(ValidationException.class, () -> service.create(task.id, task.id, DependencyType.BLOCKS));
    }

    @Test
    @TestTransaction
    void rejectsCycleAcrossThreeTasks() {
        List<Task> tasks = data.tasks(3);
        service.create(tasks.get(1).id, tasks.get(0).id, DependencyType.BLOCKS);
        service.create(tasks.get(2).id, tasks.get(1).id, DependencyType.BLOCKS);
        assertThrows(CircularDependencyException.class,
                () -> service.create(tasks.get(0).id, tasks.get(2).id, DependencyType.BLOCKS));
    }
}
```

- [ ] **Step 2: rodar o teste e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskDependencyServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar DFS antes de persistir**

```java
public final class CircularDependencyException extends DomainException {
    public CircularDependencyException() { super("CIRCULAR_DEPENDENCY", "Dependência circular detectada", 400); }
}
```

```java
@Transactional
public TaskDependency create(UUID blockedId, UUID blockingId, DependencyType type) {
    tasksService.get(blockedId);
    tasksService.get(blockingId);
    if (blockedId.equals(blockingId)) throw new ValidationException("Uma tarefa não pode bloquear a si mesma");
    if (repository.exists(blockingId, blockedId)) throw new ValidationException("Dependência já existe");
    if (type == DependencyType.BLOCKS && reaches(blockedId, blockingId, new HashSet<>()))
        throw new CircularDependencyException();
    TaskDependency dependency = new TaskDependency(blockingId, blockedId, type);
    repository.persist(dependency);
    return dependency;
}
private boolean reaches(UUID current, UUID target, Set<UUID> visited) {
    if (current.equals(target)) return true;
    if (!visited.add(current)) return false;
    return repository.listOutgoingBlocks(current).stream()
            .anyMatch(edge -> reaches(edge.blockedTaskId, target, visited));
}
```

`delete(taskId,dependencyId)` só remove quando a dependência pertence a `taskId` como bloqueante ou bloqueada; caso contrário lança `TaskNotFoundException(dependencyId)`. `list` delega a `listAllForTask` após validar a tarefa.

- [ ] **Step 4: rodar o teste e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskDependencyServiceTest" test`

Expected: PASS, 2 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error/CircularDependencyException.java src/main/java/dev/iury/lifeos/task/application/TaskDependencyService.java src/test/java/dev/iury/lifeos/task/application/TaskDependencyServiceTest.java
rtk git commit -m "feat: prevent circular task dependencies"
```

### Task 21: Máquina de estados pura

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/InvalidStatusTransitionException.java`
- Create: `src/main/java/dev/iury/lifeos/task/domain/validation/TaskStateMachine.java`
- Test: `src/test/java/dev/iury/lifeos/task/domain/validation/TaskStateMachineTest.java`

**Interfaces:**
- Produces: `void validate(TaskStatus from, TaskStatus to)` sem acesso a banco.

- [ ] **Step 1: escrever a matriz falha**

```java
class TaskStateMachineTest {
    private final TaskStateMachine machine = new TaskStateMachine();

    @ParameterizedTest
    @CsvSource({
        "TODO,IN_PROGRESS", "TODO,CANCELED",
        "IN_PROGRESS,COMPLETED", "IN_PROGRESS,BLOCKED", "IN_PROGRESS,CANCELED",
        "BLOCKED,IN_PROGRESS", "BLOCKED,CANCELED",
        "COMPLETED,TODO", "COMPLETED,CANCELED",
        "OVERDUE,IN_PROGRESS", "OVERDUE,COMPLETED", "OVERDUE,CANCELED"
    })
    void acceptsSpecifiedTransitions(TaskStatus from, TaskStatus to) {
        assertDoesNotThrow(() -> machine.validate(from, to));
    }

    @ParameterizedTest
    @CsvSource({"TODO,COMPLETED", "TODO,BLOCKED", "BLOCKED,COMPLETED", "CANCELED,TODO", "COMPLETED,IN_PROGRESS"})
    void rejectsUnspecifiedTransitions(TaskStatus from, TaskStatus to) {
        assertThrows(InvalidStatusTransitionException.class, () -> machine.validate(from, to));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskStateMachineTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar a matriz exata**

```java
public final class InvalidStatusTransitionException extends DomainException {
    public InvalidStatusTransitionException(TaskStatus from, TaskStatus to) {
        super("INVALID_STATUS_TRANSITION", "Transição de " + from + " para " + to + " não permitida", 400);
    }
}
```

```java
@ApplicationScoped
public class TaskStateMachine {
    private static final Map<TaskStatus, Set<TaskStatus>> ALLOWED = Map.of(
        TaskStatus.TODO, EnumSet.of(TaskStatus.IN_PROGRESS, TaskStatus.CANCELED),
        TaskStatus.IN_PROGRESS, EnumSet.of(TaskStatus.COMPLETED, TaskStatus.BLOCKED, TaskStatus.CANCELED),
        TaskStatus.BLOCKED, EnumSet.of(TaskStatus.IN_PROGRESS, TaskStatus.CANCELED),
        TaskStatus.COMPLETED, EnumSet.of(TaskStatus.TODO, TaskStatus.CANCELED),
        TaskStatus.CANCELED, EnumSet.noneOf(TaskStatus.class),
        TaskStatus.OVERDUE, EnumSet.of(TaskStatus.IN_PROGRESS, TaskStatus.COMPLETED, TaskStatus.CANCELED));
    public void validate(TaskStatus from, TaskStatus to) {
        if (to == null || !ALLOWED.get(from).contains(to)) throw new InvalidStatusTransitionException(from, to);
    }
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskStateMachineTest" test`

Expected: PASS, 17 casos.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error/InvalidStatusTransitionException.java src/main/java/dev/iury/lifeos/task/domain/validation/TaskStateMachine.java src/test/java/dev/iury/lifeos/task/domain/validation/TaskStateMachineTest.java
rtk git commit -m "feat: define task state machine"
```

### Task 22: Transições simples, bloqueio e auditoria

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskStatusService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskStatusServiceTest.java`

**Interfaces:**
- Produces: `Task change(UUID,TaskStatus,String)` para transições que não sejam conclusão/reabertura; `boolean hasPendingBlocker(UUID)`.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskStatusServiceTest {
    @Inject TaskStatusService service;
    @Inject ActivityLogRepository activity;
    @Inject TaskDependencyRepository dependencies;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void startsTodoAndWritesAudit() {
        Task task = data.task();
        service.change(task.id, TaskStatus.IN_PROGRESS, "começar");
        assertEquals(TaskStatus.IN_PROGRESS, task.status);
        assertEquals("começar", activity.listByTask(task.id).getLast().reason);
    }

    @Test
    @TestTransaction
    void blocksOnlyWithPendingBlocksDependency() {
        Task task = data.task(TaskStatus.IN_PROGRESS);
        assertThrows(ValidationException.class, () -> service.change(task.id, TaskStatus.BLOCKED, null));
    }

    @Test
    @TestTransaction
    void cannotStartWhileBlockerIsPending() {
        List<Task> pair = data.tasks(2);
        dependencies.persist(new TaskDependency(pair.get(0).id, pair.get(1).id, DependencyType.BLOCKS));
        assertThrows(ValidationException.class,
                () -> service.change(pair.get(1).id, TaskStatus.IN_PROGRESS, null));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskStatusServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar transação e auditoria**

```java
@Transactional
public Task change(UUID id, TaskStatus target, String reason) {
    Task task = query.get(id);
    if (target == TaskStatus.COMPLETED || (target == TaskStatus.TODO && task.status == TaskStatus.COMPLETED))
        throw new ValidationException("Use complete ou reopen para esta transição");
    machine.validate(task.status, target);
    if (target == TaskStatus.IN_PROGRESS && hasPendingBlocker(id))
        throw new ValidationException("A tarefa possui dependências bloqueantes pendentes");
    if (target == TaskStatus.BLOCKED && !hasPendingBlocker(id))
        throw new ValidationException("A tarefa só pode ser bloqueada quando existe dependência BLOCKS pendente");
    TaskStatus previous = task.status;
    task.status = target;
    activity.persist(new ActivityLog(task.id, previous, target, reason, now(task.timezone)));
    return task;
}

public boolean hasPendingBlocker(UUID taskId) {
    return dependencies.listBlocking(taskId).stream()
            .map(edge -> tasks.findById(edge.blockingTaskId))
            .anyMatch(blocker -> blocker != null && blocker.deletedAt == null && blocker.status != TaskStatus.COMPLETED);
}
```

O helper `now` usa o `Clock` da Task 16. Não permita `reason` maior que 1000 caracteres.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskStatusServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TaskStatusService.java src/test/java/dev/iury/lifeos/task/application/TaskStatusServiceTest.java
rtk git commit -m "feat: apply audited status transitions"
```

### Task 23: Cálculo da próxima ocorrência

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/recurrence/RecurrenceCalculator.java`
- Test: `src/test/java/dev/iury/lifeos/task/recurrence/RecurrenceCalculatorTest.java`

**Interfaces:**
- Produces: `Optional<LocalDateTime> next(RecurrenceRule, LocalDateTime seed, LocalDateTime currentDue, LocalDateTime completedAt, LocalDateTime now, Set<LocalDate> exceptions)`.

- [ ] **Step 1: escrever testes falhos dos cenários da Spec**

```java
class RecurrenceCalculatorTest {
    private final RecurrenceCalculator calculator = new RecurrenceCalculator();

    @Test void fixedDailySkipsPastOccurrences() {
        RecurrenceRule rule = rule("FREQ=DAILY", FIXED_SCHEDULE, null, null);
        assertEquals(LocalDateTime.of(2026,7,23,9,0), calculator.next(rule,
                at(2026,7,12), at(2026,7,12), at(2026,7,22), at(2026,7,22), Set.of()).orElseThrow());
    }

    @Test void completionBasedAnchorsOnCompletion() {
        RecurrenceRule rule = rule("FREQ=WEEKLY;INTERVAL=4", COMPLETION_BASED, null, null);
        assertEquals(LocalDateTime.of(2026,8,19,9,0), calculator.next(rule,
                at(2026,7,1), at(2026,7,1), at(2026,7,22), at(2026,7,22), Set.of()).orElseThrow());
    }

    @Test void clampsDay31ToFebruaryEnd() {
        RecurrenceRule rule = rule("FREQ=MONTHLY;BYMONTHDAY=31", FIXED_SCHEDULE, 31, null);
        assertEquals(LocalDateTime.of(2027,2,28,9,0), calculator.next(rule,
                at(2027,1,31), at(2027,1,31), at(2027,1,31), at(2027,1,31), Set.of()).orElseThrow());
    }

    @Test void overdueDay31UsesNextSeriesMonthInsteadOfSkippingOne() {
        RecurrenceRule rule = rule("FREQ=MONTHLY;BYMONTHDAY=31", FIXED_SCHEDULE, 31, null);
        assertEquals(LocalDateTime.of(2027,3,31,9,0), calculator.next(rule,
                at(2027,1,31), at(2027,1,31), at(2027,3,5), at(2027,3,5), Set.of()).orElseThrow());
    }

    @Test void skipsExceptionDate() {
        RecurrenceRule rule = rule("FREQ=DAILY", FIXED_SCHEDULE, null, null);
        assertEquals(LocalDate.of(2026,7,24), calculator.next(rule,
                at(2026,7,22), at(2026,7,22), at(2026,7,22), at(2026,7,22),
                Set.of(LocalDate.of(2026,7,23))).orElseThrow().toLocalDate());
    }

    @Test void weeklyFridayAdvancesToMonday() {
        RecurrenceRule rule = rule("FREQ=WEEKLY;BYDAY=MO,FR", FIXED_SCHEDULE, null, null);
        assertEquals(LocalDate.of(2026,7,27), calculator.next(rule,
                at(2026,7,24), at(2026,7,24), at(2026,7,24), at(2026,7,24), Set.of())
                .orElseThrow().toLocalDate());
    }

    @Test void completionBasedWeekdaysFromWeekendAdvancesToMonday() {
        RecurrenceRule rule = rule("FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR", COMPLETION_BASED, null, null);
        assertEquals(LocalDate.of(2026,7,27), calculator.next(rule,
                at(2026,7,20), at(2026,7,24), at(2026,7,26), at(2026,7,26), Set.of())
                .orElseThrow().toLocalDate());
    }

    @Test void clampsLeapDayInNonLeapYear() {
        RecurrenceRule rule = rule("FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29", FIXED_SCHEDULE, 29, 2);
        assertEquals(LocalDate.of(2027,2,28), calculator.next(rule,
                at(2024,2,29), at(2026,2,28), at(2026,2,28), at(2026,2,28), Set.of())
                .orElseThrow().toLocalDate());
    }

    @Test void stopsClampedRuleWhenCountIsExhausted() {
        RecurrenceRule rule = rule("FREQ=MONTHLY;BYMONTHDAY=31", FIXED_SCHEDULE, 31, null);
        rule.count = 1;
        assertTrue(calculator.next(rule, at(2027,1,31), at(2027,1,31),
                at(2027,1,31), at(2027,1,31), Set.of()).isEmpty());
    }

    @Test void stopsAfterUntil() {
        RecurrenceRule rule = rule("FREQ=DAILY", FIXED_SCHEDULE, null, null);
        rule.until = at(2026,7,23);
        assertTrue(calculator.next(rule, at(2026,7,22), at(2026,7,23),
                at(2026,7,23), at(2026,7,23), Set.of()).isEmpty());
    }

    private LocalDateTime at(int year, int month, int day) {
        return LocalDateTime.of(year, month, day, 9, 0);
    }

    private RecurrenceRule rule(String value, RecurrenceStrategy strategy,
            Integer byMonthDay, Integer byMonth) {
        RecurrenceFrequency frequency = RecurrenceFrequency.valueOf(
                value.substring("FREQ=".length()).split(";")[0]);
        int interval = value.contains("INTERVAL=4") ? 4 : 1;
        return new RecurrenceRule(value, frequency, interval, null, byMonthDay, byMonth,
                null, null, null, strategy, "America/Sao_Paulo");
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceCalculatorTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar ical4j mais clamp explícito**

```java
public Optional<LocalDateTime> next(RecurrenceRule rule, LocalDateTime seed,
        LocalDateTime currentDue, LocalDateTime completedAt, LocalDateTime now,
        Set<LocalDate> exceptions) {
    LocalDateTime anchor = rule.strategy == RecurrenceStrategy.COMPLETION_BASED
            ? completedAt : currentDue.isAfter(now) ? currentDue : now;
    LocalDateTime recurrenceSeed = rule.strategy == RecurrenceStrategy.COMPLETION_BASED ? completedAt : seed;
    LocalDateTime candidate = nextCandidate(rule, recurrenceSeed, anchor);
    while (candidate != null && exceptions.contains(candidate.toLocalDate()))
        candidate = nextCandidate(rule, recurrenceSeed, candidate);
    if (candidate != null && rule.until != null && candidate.isAfter(rule.until)) return Optional.empty();
    return Optional.ofNullable(candidate);
}

private LocalDateTime nextCandidate(RecurrenceRule rule, LocalDateTime seed, LocalDateTime anchor) {
    if (rule.freq == RecurrenceFrequency.MONTHLY && rule.byMonthDay != null)
        return nextClampedMonth(seed, anchor, rule.interval, rule.byMonthDay, rule.count);
    if (rule.freq == RecurrenceFrequency.YEARLY && Integer.valueOf(2).equals(rule.byMonth)
            && Integer.valueOf(29).equals(rule.byMonthDay))
        return nextClampedLeapDay(seed, anchor, rule.interval, rule.count);
    return new Recur<LocalDateTime>(rule.rruleString).getNextDate(seed, anchor);
}
```

```java
private LocalDateTime nextClampedMonth(LocalDateTime seed, LocalDateTime anchor,
        int interval, int byMonthDay, Integer count) {
    YearMonth month = YearMonth.from(seed);
    for (int attempt = 0; attempt < 10_000; attempt++, month = month.plusMonths(interval)) {
        if (count != null && attempt >= count) return null;
        int day = byMonthDay == -1 ? month.lengthOfMonth() : Math.min(byMonthDay, month.lengthOfMonth());
        LocalDateTime candidate = month.atDay(day).atTime(seed.toLocalTime());
        if (candidate.isAfter(anchor)) return candidate;
    }
    throw new InvalidRecurrenceRuleException("RRULE mensal não produziu ocorrência futura");
}

private LocalDateTime nextClampedLeapDay(LocalDateTime seed, LocalDateTime anchor,
        int interval, Integer count) {
    int year = seed.getYear();
    for (int attempt = 0; attempt < 10_000; attempt++, year += interval) {
        if (count != null && attempt >= count) return null;
        LocalDateTime candidate = LocalDate.of(year, 2, Year.isLeap(year) ? 29 : 28)
                .atTime(seed.toLocalTime());
        if (candidate.isAfter(anchor)) return candidate;
    }
    throw new InvalidRecurrenceRuleException("RRULE anual não produziu ocorrência futura");
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceCalculatorTest" test`

Expected: PASS, 10 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/recurrence/RecurrenceCalculator.java src/test/java/dev/iury/lifeos/task/recurrence/RecurrenceCalculatorTest.java
rtk git commit -m "feat: calculate recurring task dates"
```

### Task 24: Clonagem e limites da série recorrente

**Files:**
- Add method to: `src/main/java/dev/iury/lifeos/task/persistence/RecurrenceExceptionRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/RecurrenceSeriesService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/RecurrenceSeriesServiceTest.java`

**Interfaces:**
- Produces: `Optional<Task> createNext(Task current, LocalDateTime completedAt)`; copia tags e preserva raiz da série.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class RecurrenceSeriesServiceTest {
    @Inject RecurrenceSeriesService service;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void clonesTaskAndTagsWithNextDueDate() {
        Task current = data.recurring(TaskStatus.IN_PROGRESS, null, true,
                LocalDateTime.of(2026,7,22,9,0));
        Task next = service.createNext(current, LocalDateTime.of(2026,7,22,10,0)).orElseThrow();
        assertEquals(LocalDate.of(2026,7,23), next.dueDate.toLocalDate());
        assertEquals(current.id, next.parentRecurringTaskId);
        assertTrue(next.recurringMaster);
        assertEquals(TaskStatus.TODO, next.status);
    }

    @Test
    @TestTransaction
    void stopsWhenCountReached() {
        Task current = data.recurring(TaskStatus.IN_PROGRESS, 1, false,
                LocalDateTime.of(2026,7,22,9,0));
        assertTrue(service.createNext(current, LocalDateTime.of(2026,7,22,10,0)).isEmpty());
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceSeriesServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar clone mínimo**

Adicione `listDates(UUID)` a `RecurrenceExceptionRepository` retornando `Set<LocalDate>`.

Em `createNext`:

```java
if (!current.recurringMaster || current.recurrenceRuleId == null) return Optional.empty();
RecurrenceRule rule = rules.findById(current.recurrenceRuleId);
UUID rootId = current.parentRecurringTaskId == null ? current.id : current.parentRecurringTaskId;
if (rule.count != null && tasks.countSeries(rootId) >= rule.count) return Optional.empty();
LocalDateTime now = now(rule.timezone);
Optional<LocalDateTime> calculated = calculator.next(rule, rootDueDate(rootId), current.dueDate,
        completedAt, now, exceptions.listDates(rule.id));
if (calculated.isEmpty() || rule.until != null && calculated.get().isAfter(rule.until)) return Optional.empty();
if (current.parentTaskId != null) {
    Task parent = tasks.findActiveById(current.parentTaskId)
        .orElseThrow(() -> new TaskNotFoundException(current.parentTaskId));
    if (parent.dueDate != null && calculated.get().isAfter(parent.dueDate)) return Optional.empty();
}
Task clone = Task.create(current.title, current.projectId);
clone.description = current.description;
clone.priority = current.priority;
clone.dueDate = calculated.get();
clone.startDate = current.startDate == null ? null
        : current.startDate.plus(Duration.between(current.dueDate, calculated.get()));
clone.dueTime = current.dueTime;
clone.timezone = current.timezone;
clone.estimatedDuration = current.estimatedDuration;
clone.requiresPayment = current.requiresPayment;
clone.expectedAmount = current.expectedAmount;
clone.parentTaskId = current.parentTaskId;
clone.recurrenceRuleId = current.recurrenceRuleId;
clone.parentRecurringTaskId = rootId;
clone.recurringMaster = true;
tasks.persist(clone);
for (TaskTag link : taskTags.listByTask(current.id)) taskTags.persist(new TaskTag(clone.id, link.id.tagId()));
activity.persist(new ActivityLog(clone.id, null, TaskStatus.TODO, null, now(clone.timezone)));
return Optional.of(clone);
```

`rootDueDate` carrega a raiz por id e retorna seu `dueDate`; se a raiz não existir, lança `TaskNotFoundException`.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=RecurrenceSeriesServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/RecurrenceSeriesService.java src/main/java/dev/iury/lifeos/task/persistence/RecurrenceExceptionRepository.java src/test/java/dev/iury/lifeos/task/application/RecurrenceSeriesServiceTest.java
rtk git commit -m "feat: clone recurring task series"
```

### Task 25: Contratos e publisher Kafka

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/messaging/TaskCompletedEvent.java`
- Create: `src/main/java/dev/iury/lifeos/task/messaging/TaskReopenedEvent.java`
- Create: `src/main/java/dev/iury/lifeos/task/messaging/TaskEventPublisher.java`
- Test: `src/test/java/dev/iury/lifeos/task/messaging/TaskEventContractTest.java`

**Interfaces:**
- Produces: JSON exato dos dois eventos; publisher com três retries além da tentativa inicial.

- [ ] **Step 1: escrever o teste falho do JSON**

```java
class TaskEventContractTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void serializesCompletedContract() throws Exception {
        TaskCompletedEvent event = new TaskCompletedEvent("TASK_COMPLETED",
                UUID.fromString("00000000-0000-0000-0000-000000000001"), "Pagar conta de luz",
                new BigDecimal("150.00"), Instant.parse("2026-07-22T15:00:00Z"),
                Instant.parse("2026-07-22T15:00:01Z"));
        JsonNode json = mapper.readTree(mapper.writeValueAsString(event));
        assertEquals("TASK_COMPLETED", json.get("eventType").asText());
        assertEquals("00000000-0000-0000-0000-000000000001", json.get("taskId").asText());
        assertEquals("Pagar conta de luz", json.get("title").asText());
        assertEquals("150.00", json.get("expectedAmount").asText());
        assertEquals("2026-07-22T15:00:00Z", json.get("completedAt").asText());
        assertEquals("2026-07-22T15:00:01Z", json.get("timestamp").asText());
        assertEquals(6, json.size());
    }

    @Test
    void serializesReopenedContract() throws Exception {
        TaskReopenedEvent event = new TaskReopenedEvent("TASK_REOPENED",
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                Instant.parse("2026-07-22T16:00:00Z"), Instant.parse("2026-07-22T16:00:01Z"));
        JsonNode json = mapper.readTree(mapper.writeValueAsString(event));
        assertEquals("TASK_REOPENED", json.get("eventType").asText());
        assertEquals("00000000-0000-0000-0000-000000000001", json.get("taskId").asText());
        assertEquals("2026-07-22T16:00:00Z", json.get("reopenedAt").asText());
        assertEquals("2026-07-22T16:00:01Z", json.get("timestamp").asText());
        assertEquals(4, json.size());
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskEventContractTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar records e publisher**

```java
public record TaskCompletedEvent(String eventType, UUID taskId, String title,
        BigDecimal expectedAmount, Instant completedAt, Instant timestamp) {}
```

```java
public record TaskReopenedEvent(String eventType, UUID taskId, Instant reopenedAt, Instant timestamp) {}
```

```java
@ApplicationScoped
public class TaskEventPublisher {
    @Inject @Channel("task-completed") Emitter<TaskCompletedEvent> completed;
    @Inject @Channel("task-reopened") Emitter<TaskReopenedEvent> reopened;

    @Retry(maxRetries = 3, delay = 250)
    public void publishCompleted(TaskCompletedEvent event) { completed.send(event).toCompletableFuture().join(); }

    @Retry(maxRetries = 3, delay = 250)
    public void publishReopened(TaskReopenedEvent event) { reopened.send(event).toCompletableFuture().join(); }
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskEventContractTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/messaging src/test/java/dev/iury/lifeos/task/messaging/TaskEventContractTest.java
rtk git commit -m "feat: define task Kafka events"
```

### Task 26: Conclusão transacional de tarefa

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/domain/error/SubtasksNotCompletedException.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/CompletionResult.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskCompletionService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskCompletionServiceTest.java`

**Interfaces:**
- Produces: `CompletionResult complete(UUID)` contendo tarefa concluída e próxima instância opcional.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskCompletionServiceTest {
    @Inject TaskCompletionService service;
    @Inject ActivityLogRepository activity;
    @Inject PersistenceTestData data;
    @InjectMock TaskEventPublisher publisher;

    @Test
    @TestTransaction
    void rejectsParentWithOpenChild() {
        Task parent = data.inProgressWithOpenChild();
        assertThrows(SubtasksNotCompletedException.class, () -> service.complete(parent.id));
    }

    @Test
    @TestTransaction
    void completesAndReturnsRecurringClone() {
        Task task = data.recurring(TaskStatus.IN_PROGRESS, null, false,
                LocalDateTime.of(2026,7,22,9,0));
        CompletionResult result = service.complete(task.id);
        assertEquals(TaskStatus.COMPLETED, result.completed().status);
        assertNotNull(result.completed().completedAt);
        assertTrue(result.next().isPresent());
        assertEquals(TaskStatus.COMPLETED, activity.listByTask(task.id).getLast().toStatus);
    }

    @Test
    @TestTransaction
    void marksCompletionEventOnlyAfterPublisherReturns() {
        Task task = data.inProgressPaymentTask(new BigDecimal("150.00"));
        service.complete(task.id);
        verify(publisher).publishCompleted(argThat(event -> event.taskId().equals(task.id)));
        assertTrue(task.completionEventPublished);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskCompletionServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar conclusão, evento e desbloqueio**

```java
public final class SubtasksNotCompletedException extends DomainException {
    public SubtasksNotCompletedException() { super("SUBTASKS_NOT_COMPLETED", "Complete as subtarefas primeiro", 400); }
}
public record CompletionResult(Task completed, Optional<Task> next) {}
```

`complete` deve executar nesta ordem, dentro de `@Transactional`:

```java
Task task = query.get(id);
machine.validate(task.status, TaskStatus.COMPLETED);
if (status.hasPendingBlocker(id)) throw new ValidationException("A tarefa possui dependências bloqueantes pendentes");
boolean openChild = tasks.listChildren(id).stream()
        .anyMatch(child -> child.status != TaskStatus.COMPLETED && child.status != TaskStatus.CANCELED);
if (openChild) throw new SubtasksNotCompletedException();
TaskStatus previous = task.status;
LocalDateTime completedAt = now(task.timezone);
task.status = TaskStatus.COMPLETED;
task.completedAt = completedAt;
activity.persist(new ActivityLog(id, previous, TaskStatus.COMPLETED, null, completedAt));
Optional<Task> next = series.createNext(task, completedAt);
if (task.requiresPayment) {
    Instant completedInstant = completedAt.atZone(ZoneId.of(task.timezone)).toInstant();
    publisher.publishCompleted(new TaskCompletedEvent("TASK_COMPLETED", task.id, task.title,
            task.expectedAmount, completedInstant, clock.instant()));
    task.completionEventPublished = true;
}
unblockDependents(id);
return new CompletionResult(task, next);
```

`unblockDependents` percorre `dependencies.listOutgoingBlocks(id)`; para cada tarefa `BLOCKED` sem outro blocker pendente, muda para `TODO` e cria ActivityLog com reason `Dependências bloqueantes concluídas`.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskCompletionServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/domain/error/SubtasksNotCompletedException.java src/main/java/dev/iury/lifeos/task/application/CompletionResult.java src/main/java/dev/iury/lifeos/task/application/TaskCompletionService.java src/test/java/dev/iury/lifeos/task/application/TaskCompletionServiceTest.java
rtk git commit -m "feat: complete tasks transactionally"
```

### Task 27: Reabertura com evento compensatório

**Files:**
- Modify: `src/main/java/dev/iury/lifeos/task/application/TaskCompletionService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskReopenServiceTest.java`

**Interfaces:**
- Produces: `Task reopen(UUID)`; limpa `completedAt`; publica apenas quando `completionEventPublished=true`.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskReopenServiceTest {
    @Inject TaskCompletionService service;
    @Inject ActivityLogRepository activity;
    @Inject PersistenceTestData data;
    @InjectMock TaskEventPublisher publisher;

    @BeforeEach
    void resetPublisher() { reset(publisher); }

    @Test
    @TestTransaction
    void reopensCompletedTaskAndClearsTimestamp() {
        Task task = data.completed(false);
        Task reopened = service.reopen(task.id);
        assertEquals(TaskStatus.TODO, reopened.status);
        assertNull(reopened.completedAt);
        assertEquals(TaskStatus.TODO, activity.listByTask(task.id).getLast().toStatus);
        verifyNoInteractions(publisher);
    }

    @Test
    @TestTransaction
    void publishesCompensationOnlyWhenCompletionEventWasPublished() {
        Task emitted = data.completed(true);
        service.reopen(emitted.id);
        verify(publisher).publishReopened(argThat(event -> event.taskId().equals(emitted.id)));
    }

    @Test
    @TestTransaction
    void doesNotPublishWhenPaymentFlagExistsButCompletionEventWasNotPublished() {
        Task notEmitted = data.completed(true);
        notEmitted.completionEventPublished = false;
        service.reopen(notEmitted.id);
        verifyNoInteractions(publisher);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskReopenServiceTest" test`

Expected: compilation failure porque `reopen` não existe.

- [ ] **Step 3: implementar reabertura**

```java
@Transactional
public Task reopen(UUID id) {
    Task task = query.get(id);
    machine.validate(task.status, TaskStatus.TODO);
    LocalDateTime reopenedAt = now(task.timezone);
    TaskStatus previous = task.status;
    task.status = TaskStatus.TODO;
    task.completedAt = null;
    activity.persist(new ActivityLog(id, previous, TaskStatus.TODO, null, reopenedAt));
    if (task.completionEventPublished) {
        publisher.publishReopened(new TaskReopenedEvent("TASK_REOPENED", task.id,
                reopenedAt.atZone(ZoneId.of(task.timezone)).toInstant(), clock.instant()));
    }
    return task;
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskReopenServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TaskCompletionService.java src/test/java/dev/iury/lifeos/task/application/TaskReopenServiceTest.java
rtk git commit -m "feat: reopen completed tasks"
```

### Task 28: Pular ocorrência recorrente

**Files:**
- Modify: `src/main/java/dev/iury/lifeos/task/application/RecurrenceSeriesService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskSkipServiceTest.java`

**Interfaces:**
- Produces: `Task skip(UUID)`; grava EXDATE e avança `dueDate` sem ActivityLog de conclusão.

- [ ] **Step 1: escrever o teste falho**

```java
@QuarkusTest
class TaskSkipServiceTest {
    @Inject RecurrenceSeriesService service;
    @Inject RecurrenceExceptionRepository exceptions;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void recordsDueDateAndAdvancesOccurrence() {
        Task task = data.recurring(TaskStatus.TODO, null, false,
                LocalDateTime.of(2026,7,22,9,0));
        Task advanced = service.skip(task.id);
        assertTrue(exceptions.exists(task.recurrenceRuleId, LocalDate.of(2026,7,22)));
        assertEquals(LocalDate.of(2026,7,23), advanced.dueDate.toLocalDate());
        assertNull(advanced.completedAt);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskSkipServiceTest" test`

Expected: compilation failure porque `skip` não existe.

- [ ] **Step 3: implementar skip**

```java
@Transactional
public Task skip(UUID id) {
    Task task = query.get(id);
    if (task.recurrenceRuleId == null || task.dueDate == null) throw new ValidationException("Apenas tarefas recorrentes podem pular ocorrência");
    RecurrenceRule rule = rules.findById(task.recurrenceRuleId);
    LocalDate skipped = task.dueDate.toLocalDate();
    if (!exceptions.exists(rule.id, skipped))
        exceptions.persist(new RecurrenceException(rule.id, skipped, RecurrenceExceptionReason.SKIPPED));
    task.dueDate = calculator.next(rule, rootDueDate(rootId(task)), task.dueDate,
            now(rule.timezone), now(rule.timezone), exceptions.listDates(rule.id)).orElseThrow(
                () -> new ValidationException("A recorrência não possui próxima ocorrência"));
    return task;
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskSkipServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/RecurrenceSeriesService.java src/test/java/dev/iury/lifeos/task/application/TaskSkipServiceTest.java
rtk git commit -m "feat: skip recurring occurrences"
```

### Task 29: Soft delete e restauração em cascata

**Files:**
- Add methods to: `src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskTrashService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskTrashServiceTest.java`

**Interfaces:**
- Produces: `delete(UUID)` e `restore(UUID)` recursivos, preservando exclusões anteriores.

- [ ] **Step 1: escrever teste falho do lote**

```java
@QuarkusTest
class TaskTrashServiceTest {
    @Inject TaskTrashService service;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void deletesAndRestoresOnlySameCascadeBatch() {
        PersistenceTestData.TrashTree tree = data.trashTree();
        service.delete(tree.parent().id);
        LocalDateTime batch = tree.parent().deletedAt;
        assertEquals(batch, tree.activeChild().deletedAt);
        service.restore(tree.parent().id);
        assertNull(tree.parent().deletedAt);
        assertNull(tree.activeChild().deletedAt);
        assertNotNull(tree.previouslyDeletedChild().deletedAt);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskTrashServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar lote recursivo**

Adicione ao repository:

```java
public Optional<Task> findAnyById(UUID id) { return findByIdOptional(id); }
public List<Task> listAllChildren(UUID parentId) { return list("parentTaskId", parentId); }
public List<Task> listDeletedChildren(UUID parentId, LocalDateTime deletedAt) {
    return list("parentTaskId = ?1 and deletedAt = ?2", parentId, deletedAt);
}
```

`TaskTrashService`:

```java
@Transactional
public void delete(UUID id) {
    Task root = query.get(id);
    LocalDateTime batch = now(root.timezone);
    markDeleted(root, batch);
}
private void markDeleted(Task task, LocalDateTime batch) {
    task.deletedAt = batch;
    for (Task child : tasks.listAllChildren(task.id)) if (child.deletedAt == null) markDeleted(child, batch);
}
@Transactional
public void restore(UUID id) {
    Task root = tasks.findAnyById(id).orElseThrow(() -> new TaskNotFoundException(id));
    if (root.deletedAt == null) throw new ValidationException("Tarefa não está na lixeira");
    LocalDateTime batch = root.deletedAt;
    restoreBatch(root, batch);
}
private void restoreBatch(Task task, LocalDateTime batch) {
    task.deletedAt = null;
    for (Task child : tasks.listDeletedChildren(task.id, batch)) restoreBatch(child, batch);
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskTrashServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java src/main/java/dev/iury/lifeos/task/application/TaskTrashService.java src/test/java/dev/iury/lifeos/task/application/TaskTrashServiceTest.java
rtk git commit -m "feat: add recursive task trash"
```

### Task 30: Exclusão de projeto movendo tarefas para Inbox

**Files:**
- Modify: `src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java`
- Modify: `src/main/java/dev/iury/lifeos/task/application/ProjectService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/ProjectDeleteServiceTest.java`

**Interfaces:**
- Produces: `void delete(UUID)`; move tarefas ativas e apagadas antes de remover Project.

- [ ] **Step 1: escrever teste falho**

```java
@QuarkusTest
class ProjectDeleteServiceTest {
    @Inject ProjectService projects;
    @Inject TaskRepository tasks;

    @Test
    @TestTransaction
    void movesProjectTasksToInboxBeforeDelete() {
        Project source = projects.create("Casa", null);
        Task task = Task.create("Limpar", source.id);
        tasks.persist(task);
        UUID inboxId = projects.inbox().id;
        projects.delete(source.id);
        assertEquals(inboxId, tasks.findById(task.id).projectId);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectDeleteServiceTest" test`

Expected: compilation failure porque `delete` não existe.

- [ ] **Step 3: implementar update em lote**

Repository:

```java
public int moveProject(UUID source, UUID target) {
    return update("projectId = ?1 where projectId = ?2", target, source);
}
```

Service:

```java
@Transactional
public void delete(UUID id) {
    Project project = get(id).project();
    if (project.inbox) throw new ValidationException("O projeto Inbox não pode ser excluído");
    Project inbox = inbox();
    tasks.moveProject(id, inbox.id);
    projects.delete(project);
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectDeleteServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java src/main/java/dev/iury/lifeos/task/application/ProjectService.java src/test/java/dev/iury/lifeos/task/application/ProjectDeleteServiceTest.java
rtk git commit -m "feat: move tasks when deleting project"
```

### Task 31: Associação de até 30 tags

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/application/TaskTagService.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/TaskTagServiceTest.java`

**Interfaces:**
- Produces: `attach(UUID,UUID)` idempotente; `detach(UUID,UUID)` idempotente.

- [ ] **Step 1: escrever testes falhos**

```java
@QuarkusTest
class TaskTagServiceTest {
    @Inject TaskTagService service;
    @Inject TaskTagRepository links;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void attachingSameTagTwiceIsIdempotent() {
        PersistenceTestData.TaggedData fixture = data.tagged(0);
        Tag tag = data.tag("casa");
        service.attach(fixture.task().id, tag.id);
        service.attach(fixture.task().id, tag.id);
        assertEquals(1, links.countByTask(fixture.task().id));
    }

    @Test
    @TestTransaction
    void rejectsThirtyFirstTag() {
        PersistenceTestData.TaggedData fixture = data.tagged(30);
        Tag extra = data.tag("extra");
        assertThrows(ValidationException.class, () -> service.attach(fixture.task().id, extra.id));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskTagServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar limite e idempotência**

```java
@Transactional
public void attach(UUID taskId, UUID tagId) {
    tasks.get(taskId);
    tags.get(tagId);
    TaskTagId id = new TaskTagId(taskId, tagId);
    if (links.findById(id) != null) return;
    if (links.countByTask(taskId) >= 30) throw new ValidationException("Uma tarefa pode ter no máximo 30 tags");
    links.persist(new TaskTag(taskId, tagId));
}
@Transactional
public void detach(UUID taskId, UUID tagId) {
    tasks.get(taskId);
    links.deleteById(new TaskTagId(taskId, tagId));
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskTagServiceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/application/TaskTagService.java src/test/java/dev/iury/lifeos/task/application/TaskTagServiceTest.java
rtk git commit -m "feat: associate tags with tasks"
```

### Task 32: Jobs de overdue e purge

**Files:**
- Modify: `src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/OverdueService.java`
- Create: `src/main/java/dev/iury/lifeos/task/application/TrashPurgeService.java`
- Create: `src/main/java/dev/iury/lifeos/task/job/OverdueChecker.java`
- Create: `src/main/java/dev/iury/lifeos/task/job/TrashPurger.java`
- Test: `src/test/java/dev/iury/lifeos/task/application/BackgroundJobsServiceTest.java`

**Interfaces:**
- Produces: overdue a cada 15 minutos; hard delete diário às 03:00 para `deletedAt < now-30d`.

- [ ] **Step 1: escrever testes falhos dos efeitos**

```java
@QuarkusTest
class BackgroundJobsServiceTest {
    @Inject OverdueService overdue;
    @Inject TrashPurgeService purge;
    @Inject TaskRepository tasks;
    @Inject PersistenceTestData data;

    @Test
    @TestTransaction
    void marksOnlyOpenPastDueTasks() {
        PersistenceTestData.DueSet fixture = data.dueSet();
        assertEquals(2, overdue.executeAt(Instant.parse("2026-07-22T13:00:00Z")));
        assertEquals(TaskStatus.OVERDUE, fixture.todoPast().status);
        assertEquals(TaskStatus.OVERDUE, fixture.progressPast().status);
        assertEquals(TaskStatus.TODO, fixture.todoFuture().status);
    }

    @Test
    @TestTransaction
    void purgesOnlyTrashOlderThanThirtyDays() {
        PersistenceTestData.TrashSet fixture = data.trashSet();
        assertEquals(1, purge.executeAt(Instant.parse("2026-07-22T07:00:00Z")));
        assertNull(tasks.findById(fixture.old().id));
        assertNotNull(tasks.findById(fixture.recent().id));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=BackgroundJobsServiceTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar serviços e agendadores finos**

Adicione ao repository:

```java
public List<Task> listDueForOverdue(Instant now) {
    return list("deletedAt is null and dueDate < function('timezone', timezone, ?1) and status in ?2",
            now, List.of(TaskStatus.TODO, TaskStatus.IN_PROGRESS));
}
```

`OverdueService.executeAt(Instant now)` percorre a lista, altera cada status para `OVERDUE`, grava ActivityLog com reason `Prazo expirado` usando `LocalDateTime.ofInstant(now, ZoneId.of(task.timezone))` e retorna a quantidade. `TrashPurgeService.executeAt(Instant now)` chama `tasks.hardDeleteBefore(now.minus(30, ChronoUnit.DAYS))`.

```java
@ApplicationScoped
public class OverdueChecker {
    @Inject OverdueService service;
    @Scheduled(every = "15m", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void check() { service.execute(); }
}
```

```java
@ApplicationScoped
public class TrashPurger {
    @Inject TrashPurgeService service;
    @Scheduled(cron = "0 0 3 * * ?", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void purge() { service.execute(); }
}
```

Os métodos sem argumento passam `clock.instant()` aos métodos `executeAt` testáveis.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=BackgroundJobsServiceTest" test`

Expected: PASS, 2 tests.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/persistence/TaskRepository.java src/main/java/dev/iury/lifeos/task/application/OverdueService.java src/main/java/dev/iury/lifeos/task/application/TrashPurgeService.java src/main/java/dev/iury/lifeos/task/job src/test/java/dev/iury/lifeos/task/application/BackgroundJobsServiceTest.java
rtk git commit -m "feat: schedule overdue and trash jobs"
```

### Task 33: Formato padronizado de erros REST

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/error/ErrorResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/error/DomainExceptionMapper.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/error/ConstraintViolationExceptionMapper.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/error/WebApplicationExceptionMapper.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/error/UnhandledExceptionMapper.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/error/ErrorMapperTest.java`

**Interfaces:**
- Produces: `{error,message,status,timestamp}` em toda resposta 4xx/5xx.

- [ ] **Step 1: escrever teste falho do contrato**

```java
class ErrorMapperTest {
    @Test
    void mapsDomainExceptionToSpecifiedBody() {
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        DomainExceptionMapper mapper = new DomainExceptionMapper(Clock.fixed(
                Instant.parse("2026-07-22T15:00:00Z"), ZoneOffset.UTC));
        Response response = mapper.toResponse(new TaskNotFoundException(id));
        ErrorResponse error = (ErrorResponse) response.getEntity();
        assertEquals(404, response.getStatus());
        assertEquals("NOT_FOUND", error.error());
        assertEquals("2026-07-22T15:00:00Z", error.timestamp().toString());
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ErrorMapperTest" test`

Expected: compilation failure.

- [ ] **Step 3: implementar todos os mappers**

```java
public record ErrorResponse(String error, String message, int status, Instant timestamp) {}
```

```java
@Provider
public class DomainExceptionMapper implements ExceptionMapper<DomainException> {
    private final Clock clock;
    @Inject public DomainExceptionMapper(Clock clock) { this.clock = clock; }
    @Override public Response toResponse(DomainException exception) {
        return Response.status(exception.status())
                .entity(new ErrorResponse(exception.error(), exception.getMessage(), exception.status(), clock.instant()))
                .type(MediaType.APPLICATION_JSON).build();
    }
}
```

```java
@Provider
public class ConstraintViolationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {
    @Inject Clock clock;
    @Override public Response toResponse(ConstraintViolationException exception) {
        String message = exception.getConstraintViolations().stream()
            .map(ConstraintViolation::getMessage).distinct().sorted().collect(Collectors.joining("; "));
        return Response.status(400).entity(new ErrorResponse("VALIDATION_ERROR", message, 400, clock.instant()))
            .type(MediaType.APPLICATION_JSON).build();
    }
}
```

```java
@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {
    @Inject Clock clock;
    @Override public Response toResponse(WebApplicationException exception) {
        int status = exception.getResponse().getStatus();
        String code = status == 404 ? "NOT_FOUND" : "HTTP_" + status;
        String message = status == 404 ? "Recurso não encontrado"
            : Response.Status.fromStatusCode(status).getReasonPhrase();
        return Response.status(status).entity(new ErrorResponse(code, message, status, clock.instant()))
            .type(MediaType.APPLICATION_JSON).build();
    }
}
```

```java
@Provider
public class UnhandledExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger LOG = Logger.getLogger(UnhandledExceptionMapper.class);
    @Inject Clock clock;
    @Override public Response toResponse(Throwable exception) {
        LOG.error("Unhandled REST error", exception);
        return Response.serverError().entity(new ErrorResponse("INTERNAL_ERROR",
            "Erro interno do servidor", 500, clock.instant())).type(MediaType.APPLICATION_JSON).build();
    }
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ErrorMapperTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/error src/test/java/dev/iury/lifeos/task/api/error/ErrorMapperTest.java
rtk git commit -m "feat: standardize REST errors"
```

### Task 34: REST de Projects

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/CreateProjectRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/UpdateProjectRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/ProjectResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/ProjectResource.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/ProjectResourceTest.java`

**Interfaces:**
- Produces: os cinco endpoints `/api/projects` da Spec.

- [ ] **Step 1: escrever teste API falho**

```java
@QuarkusTest
class ProjectResourceTest {
    @Test
    void crudProject() {
        String location = given().contentType(JSON).body("""
                {"name":"Casa","description":"tarefas domésticas"}
                """).when().post("/api/projects").then().statusCode(201)
                .body("name", equalTo("Casa")).extract().header("Location");
        given().when().get("/api/projects").then().statusCode(200)
                .body("name", hasItem("Casa"));
        given().when().get(location).then().statusCode(200).body("taskCount", equalTo(0));
        given().contentType(JSON).body("{\"name\":\"Lar\",\"description\":null}")
                .when().put(location).then().statusCode(200).body("name", equalTo("Lar"));
        given().when().delete(location).then().statusCode(204);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectResourceTest" test`

Expected: FAIL com HTTP 404.

- [ ] **Step 3: implementar DTOs e resource**

```java
public record CreateProjectRequest(@NotBlank @Size(max=100) String name, String description) {}
public record UpdateProjectRequest(@NotBlank @Size(max=100) String name, String description) {}
public record ProjectResponse(UUID id, String name, String description, boolean inbox,
                              LocalDateTime createdAt, long taskCount) {}
```

`ProjectResource` usa `@Path("/api/projects")`, JSON e `@Valid`. POST retorna 201 + `Location: /api/projects/{id}`; GET lista chama `service.get(id)` para incluir `taskCount`; GET id, PUT e DELETE delegam ao service. O mapper privado:

```java
private ProjectResponse response(ProjectDetails details) {
    Project p = details.project();
    return new ProjectResponse(p.id, p.name, p.description, p.inbox, p.createdAt, details.taskCount());
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=ProjectResourceTest" test`

Expected: PASS, fluxo 201/200/200/204.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/dto/CreateProjectRequest.java src/main/java/dev/iury/lifeos/task/api/dto/UpdateProjectRequest.java src/main/java/dev/iury/lifeos/task/api/dto/ProjectResponse.java src/main/java/dev/iury/lifeos/task/api/resource/ProjectResource.java src/test/java/dev/iury/lifeos/task/api/resource/ProjectResourceTest.java
rtk git commit -m "feat: expose project API"
```

### Task 35: REST de Tags e associação com Task

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/CreateTagRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/TagResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TagResource.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TaskTagResource.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/TagResourceTest.java`

**Interfaces:**
- Produces: CRUD especificado de tags e `POST/DELETE /api/tasks/{id}/tags/{tagId}`.

- [ ] **Step 1: escrever teste API falho**

```java
@QuarkusTest
class TagResourceTest {
    @Inject TaskCreationService tasks;
    @Inject TaskTagRepository taskTags;

    @Test
    void createsListsAssociatesDetachesAndDeletesTag() {
        UUID id = UUID.fromString(given().contentType(JSON)
                .body("{\"name\":\"urgente\",\"color\":\"#FF5733\"}")
                .when().post("/api/tags").then().statusCode(201)
                .body("name", equalTo("urgente")).extract().path("id"));
        given().when().get("/api/tags").then().statusCode(200).body("name", hasItem("urgente"));
        Task task = tasks.create(new CreateTaskCommand("Testar tag", null, TaskPriority.P4,
                null, null, null, "America/Sao_Paulo", null, false, null,
                null, null, null));
        given().when().post("/api/tasks/{id}/tags/{tagId}", task.id, id).then().statusCode(204);
        assertEquals(1, taskTags.listByTask(task.id).size());
        given().when().delete("/api/tasks/{id}/tags/{tagId}", task.id, id).then().statusCode(204);
        assertTrue(taskTags.listByTask(task.id).isEmpty());
        given().when().delete("/api/tags/{id}", id).then().statusCode(204);
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TagResourceTest" test`

Expected: FAIL com HTTP 404.

- [ ] **Step 3: implementar DTOs e resources**

```java
public record CreateTagRequest(@NotBlank @Size(max=50) String name,
        @Pattern(regexp="^#[0-9A-Fa-f]{6}$") String color) {}
public record TagResponse(UUID id, String name, String color) {}
```

`TagResource` usa `/api/tags`: POST 201, GET 200 e DELETE 204. `TaskTagResource` usa `/api/tasks/{taskId}/tags/{tagId}`: POST chama `attach` e retorna 204; DELETE chama `detach` e retorna 204. Converta Tag para TagResponse num método privado, sem devolver entidade.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TagResourceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/dto/CreateTagRequest.java src/main/java/dev/iury/lifeos/task/api/dto/TagResponse.java src/main/java/dev/iury/lifeos/task/api/resource/TagResource.java src/main/java/dev/iury/lifeos/task/api/resource/TaskTagResource.java src/test/java/dev/iury/lifeos/task/api/resource/TagResourceTest.java
rtk git commit -m "feat: expose task tag API"
```

### Task 36: DTOs e mapper de Task

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/RecurrenceRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/CreateTaskRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/UpdateTaskRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/TaskResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/TaskDetailResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/TaskPageResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/DependencyResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/mapper/TaskMapper.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/mapper/TaskMapperTest.java`

**Interfaces:**
- Produces: conversões entidade→response e request→command sem lógica de negócio.

- [ ] **Step 1: escrever teste falho de mapeamento**

```java
class TaskMapperTest {
    private final TaskMapper mapper = new TaskMapper();

    @Test
    void mapsEveryPublicTaskField() {
        Task task = Task.create("Tarefa", UUID.fromString("00000000-0000-0000-0000-000000000010"));
        task.id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        task.description = "Descrição";
        task.priority = TaskPriority.P1;
        task.status = TaskStatus.COMPLETED;
        task.version = 3;
        task.completedAt = LocalDateTime.of(2026, 7, 22, 9, 0);
        TaskResponse response = mapper.response(task);
        assertAll(
            () -> assertEquals(task.id, response.id()),
            () -> assertEquals(task.title, response.title()),
            () -> assertEquals(task.version, response.version()),
            () -> assertEquals(task.completedAt, response.completedAt()),
            () -> assertEquals(task.deletedAt, response.deletedAt()));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskMapperTest" test`

Expected: compilation failure.

- [ ] **Step 3: criar records completos e mapper**

```java
public record RecurrenceRequest(@NotBlank String rruleString,
        @NotNull RecurrenceStrategy strategy, @NotBlank String timezone) {}

public record CreateTaskRequest(
        @NotBlank @Size(max=255) String title,
        String description,
        TaskPriority priority,
        LocalDateTime startDate,
        LocalDateTime dueDate,
        LocalTime dueTime,
        String timezone,
        @Min(1) @Max(525600) Integer estimatedDuration,
        boolean requiresPayment,
        BigDecimal expectedAmount,
        UUID projectId,
        UUID parentTaskId,
        @Valid RecurrenceRequest recurrence) {}

public record UpdateTaskRequest(
        @NotNull Integer version,
        @NotBlank @Size(max=255) String title,
        String description,
        TaskPriority priority,
        LocalDateTime startDate,
        LocalDateTime dueDate,
        LocalTime dueTime,
        String timezone,
        @Min(1) @Max(525600) Integer estimatedDuration,
        boolean requiresPayment,
        BigDecimal expectedAmount) {}

public record TaskResponse(
        UUID id, String title, String description, TaskStatus status, TaskPriority priority,
        LocalDateTime startDate, LocalDateTime dueDate, LocalTime dueTime, String timezone,
        Integer estimatedDuration, boolean requiresPayment, BigDecimal expectedAmount,
        UUID projectId, UUID parentTaskId, UUID recurrenceRuleId, UUID parentRecurringTaskId,
        boolean recurringMaster, Integer version, LocalDateTime completedAt,
        LocalDateTime deletedAt, LocalDateTime createdAt, LocalDateTime updatedAt) {}
```

```java
public record DependencyResponse(UUID id, UUID blockingTaskId, UUID blockedTaskId, DependencyType type) {}
public record TaskDetailResponse(TaskResponse task, List<TaskResponse> subtasks,
        List<TagResponse> tags, List<DependencyResponse> dependencies) {}
public record TaskPageResponse(List<TaskResponse> items, long total, int page, int size) {}
```

```java
@ApplicationScoped
public class TaskMapper {
    public TaskResponse response(Task task) {
        return new TaskResponse(task.id, task.title, task.description, task.status, task.priority,
            task.startDate, task.dueDate, task.dueTime, task.timezone, task.estimatedDuration,
            task.requiresPayment, task.expectedAmount, task.projectId, task.parentTaskId,
            task.recurrenceRuleId, task.parentRecurringTaskId, task.recurringMaster, task.version,
            task.completedAt, task.deletedAt, task.createdAt, task.updatedAt);
    }

    public DependencyResponse dependency(TaskDependency dependency) {
        return new DependencyResponse(dependency.id, dependency.blockingTaskId,
            dependency.blockedTaskId, dependency.type);
    }

    public TaskDetailResponse detail(TaskDetails details) {
        return new TaskDetailResponse(response(details.task()),
            details.subtasks().stream().map(this::response).toList(),
            details.tags().stream().map(tag -> new TagResponse(tag.id, tag.name, tag.color)).toList(),
            details.dependencies().stream().map(this::dependency).toList());
    }

    public TaskPageResponse page(PageResult<Task> page) {
        return new TaskPageResponse(page.items().stream().map(this::response).toList(),
            page.total(), page.page(), page.size());
    }

    public CreateTaskCommand createCommand(CreateTaskRequest request) {
        RecurrenceInput recurrence = request.recurrence() == null ? null
            : new RecurrenceInput(request.recurrence().rruleString(), request.recurrence().strategy(),
                                  request.recurrence().timezone());
        return new CreateTaskCommand(request.title(), request.description(), request.priority(),
            request.startDate(), request.dueDate(), request.dueTime(), request.timezone(),
            request.estimatedDuration(), request.requiresPayment(), request.expectedAmount(),
            request.projectId(), request.parentTaskId(), recurrence);
    }

    public UpdateTaskCommand updateCommand(UpdateTaskRequest request) {
        return new UpdateTaskCommand(request.version(), request.title(), request.description(),
            request.priority(), request.startDate(), request.dueDate(), request.dueTime(),
            request.timezone(), request.estimatedDuration(), request.requiresPayment(),
            request.expectedAmount());
    }
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskMapperTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/dto src/main/java/dev/iury/lifeos/task/api/mapper/TaskMapper.java src/test/java/dev/iury/lifeos/task/api/mapper/TaskMapperTest.java
rtk git commit -m "feat: map task API contracts"
```

### Task 37: REST de CRUD, filtros e paginação de Task

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TaskResource.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/TaskResourceTest.java`

**Interfaces:**
- Produces: `POST/GET /api/tasks`, `GET/PUT/DELETE /api/tasks/{id}`.
- Query params: `status`, `priority`, `projectId`, `tagId`, `dueDateFrom`, `dueDateTo`, `page`, `size`.

- [ ] **Step 1: escrever teste API falho**

```java
@QuarkusTest
class TaskResourceTest {
    @Test
    void createsReadsFiltersUpdatesAndDeletesTask() {
        ExtractableResponse<Response> created = given().contentType(JSON).body("""
            {"title":"Pagar luz","description":"<b>Conta</b>","priority":"P1",
             "dueDate":"2026-07-25T18:00:00","timezone":"America/Sao_Paulo",
             "requiresPayment":true,"expectedAmount":150.00}
            """).when().post("/api/tasks").then().statusCode(201)
            .body("description", equalTo("Conta")).extract();
        String location = created.header("Location");
        int version = created.path("version");
        given().when().get(location).then().statusCode(200).body("task.title", equalTo("Pagar luz"));
        given().queryParam("priority", "P1").queryParam("page", 0).queryParam("size", 20)
            .when().get("/api/tasks").then().statusCode(200).body("total", greaterThanOrEqualTo(1));
        given().contentType(JSON).body("""
            {"version":%d,"title":"Pagar energia","priority":"P2",
             "dueDate":"2026-07-25T18:00:00","timezone":"America/Sao_Paulo",
             "requiresPayment":true,"expectedAmount":150.00}
            """.formatted(version)).when().put(location).then().statusCode(200)
            .body("title", equalTo("Pagar energia"));
        given().when().delete(location).then().statusCode(204);
        given().when().get(location).then().statusCode(404).body("error", equalTo("NOT_FOUND"));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskResourceTest" test`

Expected: FAIL com HTTP 404.

- [ ] **Step 3: implementar resource**

`TaskResource` usa `@Path("/api/tasks")`, `@Produces/@Consumes(APPLICATION_JSON)` e:

```java
@POST public Response create(@Valid CreateTaskRequest request) {
    TaskResponse response = mapper.response(creation.create(mapper.createCommand(request)));
    return Response.created(URI.create("/api/tasks/" + response.id())).entity(response).build();
}
@GET public TaskPageResponse list(@QueryParam("status") TaskStatus status,
        @QueryParam("priority") TaskPriority priority, @QueryParam("projectId") UUID projectId,
        @QueryParam("tagId") UUID tagId, @QueryParam("dueDateFrom") LocalDateTime dueFrom,
        @QueryParam("dueDateTo") LocalDateTime dueTo,
        @DefaultValue("0") @QueryParam("page") int page,
        @DefaultValue("20") @QueryParam("size") int size) {
    return mapper.page(query.list(new TaskFilter(status, priority, projectId, tagId, dueFrom, dueTo), page, size));
}
@GET @Path("/{id}") public TaskDetailResponse get(@PathParam("id") UUID id) { return mapper.detail(query.details(id)); }
@PUT @Path("/{id}") public TaskResponse update(@PathParam("id") UUID id, @Valid UpdateTaskRequest request) {
    return mapper.response(updates.update(id, mapper.updateCommand(request)));
}
@DELETE @Path("/{id}") public Response delete(@PathParam("id") UUID id) {
    trash.delete(id); return Response.noContent().build();
}
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskResourceTest" test`

Expected: PASS no fluxo 201/200/200/200/204/404.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/resource/TaskResource.java src/test/java/dev/iury/lifeos/task/api/resource/TaskResourceTest.java
rtk git commit -m "feat: expose task CRUD API"
```

### Task 38: REST de ações, lixeira e overdue

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/UpdateStatusRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/CompletionResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TaskActionResource.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/TaskActionResourceTest.java`

**Interfaces:**
- Produces: `PUT /api/tasks/{id}/status`, `POST /api/tasks/{id}/complete`, `POST /api/tasks/{id}/skip`, `POST /api/tasks/{id}/reopen`, `POST /api/tasks/{id}/restore`, `GET /api/tasks/trash` e `GET /api/tasks/overdue`.

- [ ] **Step 1: escrever teste API falho**

```java
@QuarkusTest
class TaskActionResourceTest {
    @Inject OverdueService overdue;

    @Test
    void startsCompletesAndReopensTask() {
        String location = given().contentType(JSON)
            .body("{\"title\":\"Ação\",\"timezone\":\"America/Sao_Paulo\",\"requiresPayment\":false}")
            .post("/api/tasks").then().statusCode(201).extract().header("Location");
        given().contentType(JSON).body("{\"status\":\"IN_PROGRESS\",\"reason\":\"início\"}")
            .when().put(location + "/status").then().statusCode(200).body("status", equalTo("IN_PROGRESS"));
        given().when().post(location + "/complete").then().statusCode(200)
            .body("completed.status", equalTo("COMPLETED"));
        given().when().post(location + "/reopen").then().statusCode(200).body("status", equalTo("TODO"));
    }

    @Test
    void skipsRecurringOccurrence() {
        String location = given().contentType(JSON).body("""
            {"title":"Diária","dueDate":"2026-07-22T09:00:00","timezone":"America/Sao_Paulo",
             "requiresPayment":false,
             "recurrence":{"rruleString":"FREQ=DAILY","strategy":"FIXED_SCHEDULE",
                           "timezone":"America/Sao_Paulo"}}
            """).post("/api/tasks").then().statusCode(201).extract().header("Location");
        given().when().post(location + "/skip").then().statusCode(200)
            .body("status", equalTo("TODO"))
            .body("dueDate", equalTo("2026-07-23T09:00:00"));
    }

    @Test
    void listsTrashAndRestoresTask() {
        String location = createTask("Lixeira", null);
        String id = location.substring("/api/tasks/".length());
        given().when().delete(location).then().statusCode(204);
        given().when().get("/api/tasks/trash").then().statusCode(200).body("id", hasItem(id));
        given().when().post(location + "/restore").then().statusCode(200).body("id", equalTo(id));
        given().when().get(location).then().statusCode(200);
    }

    @Test
    void listsTasksMarkedOverdue() {
        String location = createTask("Atrasada", "2026-07-21T08:00:00");
        String id = location.substring("/api/tasks/".length());
        overdue.execute();
        given().when().get("/api/tasks/overdue").then().statusCode(200)
            .body("id", hasItem(id)).body("status", hasItem("OVERDUE"));
    }

    private String createTask(String title, String dueDate) {
        String due = dueDate == null ? "null" : "\"" + dueDate + "\"";
        return given().contentType(JSON).body("""
            {"title":"%s","dueDate":%s,"timezone":"America/Sao_Paulo","requiresPayment":false}
            """.formatted(title, due)).post("/api/tasks").then().statusCode(201)
            .extract().header("Location");
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskActionResourceTest" test`

Expected: FAIL com HTTP 404.

- [ ] **Step 3: implementar os endpoints literais antes do path param**

```java
public record UpdateStatusRequest(@NotNull TaskStatus status, @Size(max=1000) String reason) {}
public record CompletionResponse(TaskResponse completed, TaskResponse next) {}
```

`TaskActionResource` usa `@Path("/api/tasks")` e implementa:

```java
@PUT @Path("/{id}/status")
public TaskResponse status(@PathParam("id") UUID id, @Valid UpdateStatusRequest request) {
    Task current = query.get(id);
    if (request.status() == TaskStatus.COMPLETED) return mapper.response(completion.complete(id).completed());
    if (current.status == TaskStatus.COMPLETED && request.status() == TaskStatus.TODO) return mapper.response(completion.reopen(id));
    return mapper.response(status.change(id, request.status(), request.reason()));
}
@POST @Path("/{id}/complete") public CompletionResponse complete(@PathParam("id") UUID id) {
    CompletionResult result = completion.complete(id);
    return new CompletionResponse(mapper.response(result.completed()), result.next().map(mapper::response).orElse(null));
}
@POST @Path("/{id}/skip") public TaskResponse skip(@PathParam("id") UUID id) { return mapper.response(series.skip(id)); }
@POST @Path("/{id}/reopen") public TaskResponse reopen(@PathParam("id") UUID id) { return mapper.response(completion.reopen(id)); }
@POST @Path("/{id}/restore") public TaskResponse restore(@PathParam("id") UUID id) { trash.restore(id); return mapper.response(query.get(id)); }
@GET @Path("/trash") public List<TaskResponse> trash() { return query.trash().stream().map(mapper::response).toList(); }
@GET @Path("/overdue") public List<TaskResponse> overdue() { return query.overdue().stream().map(mapper::response).toList(); }
```

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskActionResourceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/dto/UpdateStatusRequest.java src/main/java/dev/iury/lifeos/task/api/dto/CompletionResponse.java src/main/java/dev/iury/lifeos/task/api/resource/TaskActionResource.java src/test/java/dev/iury/lifeos/task/api/resource/TaskActionResourceTest.java
rtk git commit -m "feat: expose task lifecycle API"
```

### Task 39: REST de dependências e histórico

**Files:**
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/CreateDependencyRequest.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/dto/ActivityLogResponse.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TaskDependencyResource.java`
- Create: `src/main/java/dev/iury/lifeos/task/api/resource/TaskHistoryResource.java`
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/TaskRelationResourceTest.java`

**Interfaces:**
- Produces: três endpoints de dependências e `GET /api/tasks/{id}/history`.

- [ ] **Step 1: escrever teste API falho**

```java
@QuarkusTest
class TaskRelationResourceTest {
    @Test
    void createsListsDeletesDependencyAndListsHistory() {
        UUID blocker = createTaskId("Bloqueante");
        UUID blocked = createTaskId("Bloqueada");
        UUID depId = UUID.fromString(given().contentType(JSON)
            .body("{\"blockingTaskId\":\"%s\",\"type\":\"BLOCKS\"}".formatted(blocker))
            .when().post("/api/tasks/{id}/dependencies", blocked).then().statusCode(201)
            .extract().path("id"));
        given().when().get("/api/tasks/{id}/dependencies", blocked).then().statusCode(200).body("size()", equalTo(1));
        given().contentType(JSON).body("{\"status\":\"IN_PROGRESS\"}")
            .when().put("/api/tasks/{id}/status", blocker).then().statusCode(200);
        given().when().get("/api/tasks/{id}/history", blocker).then().statusCode(200)
            .body("toStatus", hasItem("IN_PROGRESS"));
        given().when().delete("/api/tasks/{id}/dependencies/{depId}", blocked, depId).then().statusCode(204);
    }

    private UUID createTaskId(String title) {
        String location = given().contentType(JSON)
            .body("{\"title\":\"%s\",\"timezone\":\"America/Sao_Paulo\",\"requiresPayment\":false}".formatted(title))
            .post("/api/tasks").then().statusCode(201).extract().header("Location");
        return UUID.fromString(location.substring("/api/tasks/".length()));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationResourceTest" test`

Expected: FAIL com HTTP 404.

- [ ] **Step 3: implementar DTOs e resources**

```java
public record CreateDependencyRequest(@NotNull UUID blockingTaskId, @NotNull DependencyType type) {}
public record ActivityLogResponse(UUID id, UUID taskId, TaskStatus fromStatus,
        TaskStatus toStatus, String reason, LocalDateTime timestamp) {}
```

`TaskDependencyResource` usa `/api/tasks/{taskId}/dependencies`: POST retorna 201 e `DependencyResponse`, GET retorna lista, DELETE 204. `TaskHistoryResource` valida a tarefa via `TaskQueryService.get`, então transforma cada `ActivityLog` de `ActivityLogRepository.listByTask` em `ActivityLogResponse` sem permitir escrita.

- [ ] **Step 4: rodar e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskRelationResourceTest" test`

Expected: PASS.

- [ ] **Step 5: commit**

```powershell
rtk git add src/main/java/dev/iury/lifeos/task/api/dto/CreateDependencyRequest.java src/main/java/dev/iury/lifeos/task/api/dto/ActivityLogResponse.java src/main/java/dev/iury/lifeos/task/api/resource/TaskDependencyResource.java src/main/java/dev/iury/lifeos/task/api/resource/TaskHistoryResource.java src/test/java/dev/iury/lifeos/task/api/resource/TaskRelationResourceTest.java
rtk git commit -m "feat: expose dependency and history API"
```

### Task 40: Cenários REST de erro, concorrência e filtros combinados

**Files:**
- Test: `src/test/java/dev/iury/lifeos/task/api/resource/TaskApiAcceptanceTest.java`
- Create: `src/test/java/dev/iury/lifeos/task/support/ApiFixtures.java`

**Interfaces:**
- Verifies: 400, 404, 409; filtros combinados; paginação; profundidade; ciclo; payment; datas.

- [ ] **Step 1: escrever a suíte falha de aceitação**

Crie a classe abaixo antes de criar `ApiFixtures`, para que o primeiro ciclo seja vermelho por referência ausente:

```java
@QuarkusTest
class TaskApiAcceptanceTest {
    @Test
    void returns404ForMissingTask() {
        given().get("/api/tasks/{id}", UUID.randomUUID()).then().statusCode(404)
            .body("error", equalTo("NOT_FOUND"));
    }

    @Test
    void returns400ForBlankTitle() {
        given().contentType(JSON).body("{\"title\":\"   \"}").post("/api/tasks")
            .then().statusCode(400).body("error", equalTo("VALIDATION_ERROR"));
    }

    @Test
    void returns400ForStartAfterDue() {
        given().contentType(JSON).body("""
            {"title":"Datas","startDate":"2026-07-23T10:00:00",
             "dueDate":"2026-07-22T10:00:00","timezone":"America/Sao_Paulo",
             "requiresPayment":false}
            """).post("/api/tasks").then().statusCode(400)
            .body("message", equalTo("startDate deve ser menor ou igual a dueDate"));
    }

    @Test
    void returns400ForAmountWithoutPayment() {
        given().contentType(JSON).body("""
            {"title":"Valor","timezone":"America/Sao_Paulo",
             "requiresPayment":false,"expectedAmount":1.00}
            """).post("/api/tasks").then().statusCode(400)
            .body("message", equalTo("expectedAmount exige requiresPayment=true"));
    }

    @Test
    void returns409ForStaleVersion() {
        UUID id = ApiFixtures.createTask("Concorrência", "P4", null, null, "2026-07-25T10:00:00");
        int version = ApiFixtures.version(id);
        given().contentType(JSON).body(updateBody(version, "Primeira"))
            .put("/api/tasks/{id}", id).then().statusCode(200);
        given().contentType(JSON).body(updateBody(version, "Segunda"))
            .put("/api/tasks/{id}", id).then().statusCode(409)
            .body("error", equalTo("OPTIMISTIC_LOCK"));
    }

    @Test
    void combinesStatusPriorityProjectTagAndDueRange() {
        String suffix = UUID.randomUUID().toString();
        UUID project = ApiFixtures.createProjectId("Projeto-" + suffix);
        UUID otherProject = ApiFixtures.createProjectId("Outro-" + suffix);
        UUID tag = ApiFixtures.createTagId("tag-" + suffix);
        UUID matching = ApiFixtures.createTask("Correta", "P1", project, null, "2026-07-25T10:00:00");
        ApiFixtures.attachTag(matching, tag);
        ApiFixtures.createTask("Prioridade errada", "P2", project, null, "2026-07-25T10:00:00");
        ApiFixtures.createTask("Projeto errado", "P1", otherProject, null, "2026-07-25T10:00:00");

        given().queryParam("status", "TODO").queryParam("priority", "P1")
            .queryParam("projectId", project).queryParam("tagId", tag)
            .queryParam("dueDateFrom", "2026-07-25T00:00:00")
            .queryParam("dueDateTo", "2026-07-25T23:59:59")
            .queryParam("page", 0).queryParam("size", 20)
            .get("/api/tasks").then().statusCode(200).body("total", equalTo(1))
            .body("items[0].title", equalTo("Correta"));
    }

    @Test
    void rejectsPageSizeAbove100() {
        given().queryParam("size", 101).get("/api/tasks").then().statusCode(400)
            .body("error", equalTo("VALIDATION_ERROR"));
    }

    @Test
    void rejectsSixthSubtaskLevel() {
        UUID project = ApiFixtures.createProjectId("Profundidade-" + UUID.randomUUID());
        UUID parent = ApiFixtures.createTask("Nível 1", "P4", project, null, "2026-07-30T10:00:00");
        for (int level = 2; level <= 5; level++)
            parent = ApiFixtures.createTask("Nível " + level, "P4", project, parent, "2026-07-30T10:00:00");
        given().contentType(JSON).body("""
            {"title":"Nível 6","priority":"P4","projectId":"%s","parentTaskId":"%s",
             "dueDate":"2026-07-30T10:00:00","timezone":"America/Sao_Paulo","requiresPayment":false}
            """.formatted(project, parent)).post("/api/tasks").then().statusCode(400)
            .body("error", equalTo("MAX_SUBTASK_DEPTH"));
    }

    @Test
    void rejectsCircularBlocksDependency() {
        UUID a = ApiFixtures.createTaskId("A-" + UUID.randomUUID());
        UUID b = ApiFixtures.createTaskId("B-" + UUID.randomUUID());
        UUID c = ApiFixtures.createTaskId("C-" + UUID.randomUUID());
        ApiFixtures.dependency(b, a);
        ApiFixtures.dependency(c, b);
        given().contentType(JSON)
            .body("{\"blockingTaskId\":\"%s\",\"type\":\"BLOCKS\"}".formatted(c))
            .post("/api/tasks/{id}/dependencies", a).then().statusCode(400)
            .body("error", equalTo("CIRCULAR_DEPENDENCY"));
    }

    private String updateBody(int version, String title) {
        return """
            {"version":%d,"title":"%s","priority":"P4","dueDate":"2026-07-25T10:00:00",
             "timezone":"America/Sao_Paulo","requiresPayment":false}
            """.formatted(version, title);
    }
}
```

- [ ] **Step 2: rodar a suíte e registrar o primeiro comportamento divergente**

Run: `rtk .\mvnw.cmd "-Dtest=TaskApiAcceptanceTest" test`

Expected: compilation failure com `cannot find symbol: ApiFixtures`.

- [ ] **Step 3: implementar o helper mínimo da suíte**

```java
package dev.iury.lifeos.task.support;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.JSON;
import java.util.UUID;

public final class ApiFixtures {
    private ApiFixtures() {}
    public static String createTask(String title) {
        return given().contentType(JSON).body("""
            {"title":"%s","timezone":"America/Sao_Paulo","requiresPayment":false}
            """.formatted(title)).post("/api/tasks").then().statusCode(201).extract().header("Location");
    }
    public static UUID createTaskId(String title) {
        return UUID.fromString(createTask(title).substring("/api/tasks/".length()));
    }
    public static UUID createProjectId(String name) {
        return UUID.fromString(given().contentType(JSON).body("{\"name\":\"%s\"}".formatted(name))
            .post("/api/projects").then().statusCode(201).extract().path("id"));
    }
    public static UUID createTagId(String name) {
        return UUID.fromString(given().contentType(JSON)
            .body("{\"name\":\"%s\",\"color\":\"#123456\"}".formatted(name))
            .post("/api/tags").then().statusCode(201).extract().path("id"));
    }
    public static UUID createTask(String title, String priority, UUID projectId,
            UUID parentTaskId, String dueDate) {
        String body = """
            {"title":"%s","priority":"%s","projectId":%s,"parentTaskId":%s,
             "dueDate":"%s","timezone":"America/Sao_Paulo","requiresPayment":false}
            """.formatted(title, priority, jsonUuid(projectId), jsonUuid(parentTaskId), dueDate);
        return UUID.fromString(given().contentType(JSON).body(body).post("/api/tasks")
            .then().statusCode(201).extract().path("id"));
    }
    public static int version(UUID taskId) {
        return given().get("/api/tasks/{id}", taskId).then().statusCode(200).extract().path("task.version");
    }
    public static void attachTag(UUID taskId, UUID tagId) {
        given().post("/api/tasks/{id}/tags/{tagId}", taskId, tagId).then().statusCode(204);
    }
    public static UUID dependency(UUID blockedId, UUID blockingId) {
        return UUID.fromString(given().contentType(JSON)
            .body("{\"blockingTaskId\":\"%s\",\"type\":\"BLOCKS\"}".formatted(blockingId))
            .post("/api/tasks/{id}/dependencies", blockedId).then().statusCode(201).extract().path("id"));
    }
    private static String jsonUuid(UUID value) {
        return value == null ? "null" : "\"" + value + "\"";
    }
}
```

- [ ] **Step 4: rodar a suíte e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskApiAcceptanceTest" test`

Expected: PASS, 9 tests, 0 failures.

- [ ] **Step 5: commit**

```powershell
rtk git add src/test/java/dev/iury/lifeos/task/api/resource/TaskApiAcceptanceTest.java src/test/java/dev/iury/lifeos/task/support/ApiFixtures.java
rtk git commit -m "test: cover task API acceptance scenarios"
```

### Task 41: Integração real PostgreSQL, Kafka e recorrência

**Files:**
- Test: `src/test/java/dev/iury/lifeos/task/integration/TaskKafkaIntegrationTest.java`
- Test: `src/test/java/dev/iury/lifeos/task/integration/RecurringTaskIntegrationTest.java`
- Create: `src/test/java/dev/iury/lifeos/task/support/KafkaTestSupport.java`

**Interfaces:**
- Verifies: PostgreSQL/Flyway via Dev Services/Testcontainers; broker Kafka real; payloads dos dois tópicos; recorrência fim a fim.

- [ ] **Step 1: escrever testes de integração falhos**

```java
@QuarkusTest
class TaskKafkaIntegrationTest {
    @Inject TaskCompletionService completion;
    @Inject PersistenceTestData data;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    @TestTransaction
    void publishesCompletedAndReopenedEvents() {
        try (KafkaConsumer<String,String> completed = KafkaTestSupport.consumer("task.completed.events");
             KafkaConsumer<String,String> reopened = KafkaTestSupport.consumer("task.reopened.events")) {
            Task task = data.inProgressPaymentTask(new BigDecimal("150.00"));
            completion.complete(task.id);
            JsonNode completedJson = KafkaTestSupport.oneRecord(completed, mapper, Duration.ofSeconds(10));
            assertEquals("TASK_COMPLETED", completedJson.get("eventType").asText());
            assertEquals(task.id.toString(), completedJson.get("taskId").asText());
            assertEquals(new BigDecimal("150.00"), completedJson.get("expectedAmount").decimalValue());
            completion.reopen(task.id);
            assertEquals("TASK_REOPENED", KafkaTestSupport.oneRecord(reopened, mapper, Duration.ofSeconds(10))
                    .get("eventType").asText());
        }
    }
}
```

```java
@QuarkusTest
class RecurringTaskIntegrationTest {
    @Test
    void createsCompletesAndClonesDailyTask() {
        ExtractableResponse<Response> created = given().contentType(JSON).body("""
            {"title":"Diária","dueDate":"2026-07-22T09:00:00","timezone":"America/Sao_Paulo",
             "requiresPayment":false,
             "recurrence":{"rruleString":"FREQ=DAILY","strategy":"FIXED_SCHEDULE",
                           "timezone":"America/Sao_Paulo"}}
            """).post("/api/tasks").then().statusCode(201).extract();
        String location = created.header("Location");
        String rootId = created.path("id");
        given().contentType(JSON).body("{\"status\":\"IN_PROGRESS\"}")
            .put(location + "/status").then().statusCode(200);
        given().post(location + "/complete").then().statusCode(200)
            .body("completed.status", equalTo("COMPLETED"))
            .body("next.status", equalTo("TODO"))
            .body("next.parentRecurringTaskId", equalTo(rootId))
            .body("next.dueDate", equalTo("2026-07-23T09:00:00"));
    }
}
```

- [ ] **Step 2: rodar com Docker e confirmar a falha inicial**

Run: `rtk .\mvnw.cmd "-Dtest=TaskKafkaIntegrationTest,RecurringTaskIntegrationTest" test`

Expected: compilation failure com `cannot find symbol: KafkaTestSupport`; os logs anteriores da suíte já devem mostrar PostgreSQL Dev Services.

- [ ] **Step 3: implementar o helper Kafka mínimo**

```java
package dev.iury.lifeos.task.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;

public final class KafkaTestSupport {
    private KafkaTestSupport() {}

    public static KafkaConsumer<String,String> consumer(String topic) {
        String bootstrap = ConfigProvider.getConfig().getValue("kafka.bootstrap.servers", String.class);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(Map.of(
            "bootstrap.servers", bootstrap,
            "group.id", "task-service-test-" + UUID.randomUUID(),
            "auto.offset.reset", "earliest",
            "key.deserializer", StringDeserializer.class.getName(),
            "value.deserializer", StringDeserializer.class.getName()));
        consumer.subscribe(java.util.List.of(topic));
        consumer.poll(Duration.ofMillis(100));
        return consumer;
    }

    public static JsonNode oneRecord(KafkaConsumer<String,String> consumer,
            ObjectMapper mapper, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                try { return mapper.readTree(records.iterator().next().value()); }
                catch (java.io.IOException exception) {
                    throw new AssertionError("Evento Kafka contém JSON inválido", exception);
                }
            }
        }
        throw new AssertionError("Evento Kafka não recebido");
    }
}
```

Mantenha os canais de `application.properties`, o ack síncrono do publisher e as transações existentes. Não adicione outbox, schema registry ou consumidor no Task Service.

- [ ] **Step 4: rodar novamente e confirmar sucesso**

Run: `rtk .\mvnw.cmd "-Dtest=TaskKafkaIntegrationTest,RecurringTaskIntegrationTest" test`

Expected: PASS com eventos nos tópicos corretos e clone persistido no PostgreSQL.

- [ ] **Step 5: commit**

```powershell
rtk git add src/test/java/dev/iury/lifeos/task/integration src/test/java/dev/iury/lifeos/task/support/KafkaTestSupport.java
rtk git commit -m "test: verify task persistence Kafka and recurrence"
```

### Task 42: Documentação e verificação final

**Files:**
- Create: `README.md`
- Test: `src/test/java/dev/iury/lifeos/task/DocumentationTest.java`

**Interfaces:**
- Produces: instruções reproduzíveis de build, teste, dev, configuração e endpoints.

- [ ] **Step 1: escrever teste falho da documentação mínima**

```java
class DocumentationTest {
    @Test
    void readmeDocumentsRequiredOperations() throws Exception {
        String readme = Files.readString(Path.of("README.md"));
        assertAll(
            () -> assertTrue(readme.contains("Java 21")),
            () -> assertTrue(readme.contains(".\\mvnw.cmd quarkus:dev")),
            () -> assertTrue(readme.contains("TASKS_DB_URL")),
            () -> assertTrue(readme.contains("KAFKA_BOOTSTRAP_SERVERS")),
            () -> assertTrue(readme.contains("/api/tasks")),
            () -> assertTrue(readme.contains("task.completed.events")));
    }
}
```

- [ ] **Step 2: rodar e confirmar a falha**

Run: `rtk .\mvnw.cmd "-Dtest=DocumentationTest" test`

Expected: FAIL porque `README.md` não existe.

- [ ] **Step 3: escrever README real**

Documente: pré-requisitos Java 21/Maven Wrapper/Docker; `rtk .\mvnw.cmd test`; `rtk .\mvnw.cmd quarkus:dev`; porta 8081; variáveis `TASKS_DB_URL`, `TASKS_DB_USERNAME`, `TASKS_DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`; migrations automáticas; tabela de todos os endpoints das seções 10.1–10.5; tópicos e exemplos completos dos dois payloads; decisões single-user e strict; como rodar testes focados.

- [ ] **Step 4: rodar toda a verificação**

```powershell
rtk .\mvnw.cmd test
rtk .\mvnw.cmd verify
rtk git diff --check
rtk git status --short
```

Expected: todos os testes PASS; build SUCCESS; `git diff --check` sem saída; apenas README/teste ainda não commitados.

- [ ] **Step 5: commit**

```powershell
rtk git add README.md src/test/java/dev/iury/lifeos/task/DocumentationTest.java
rtk git commit -m "docs: document task service operations"
```

## Matriz de cobertura da Spec

| Spec | Implementação/teste |
|---|---|
| Stack, Java 21, Quarkus, PostgreSQL, Flyway | Tasks 1–3 |
| Todas as entidades e constraints | Tasks 2–7 |
| Listagem, visibilidade por `startDate`, filtros, paginação | Tasks 10, 18, 37, 40 |
| Validações de campos e XSS | Tasks 12, 16, 40 |
| Projetos e Inbox | Tasks 8, 13, 30, 34 |
| Tags e máximo 30 | Tasks 8, 11, 14, 31, 35 |
| Subtarefas, profundidade, strict, cascade | Tasks 17, 26, 29, 40 |
| Dependências, DAG, bloqueio e desbloqueio | Tasks 20–22, 26, 39, 40 |
| Máquina de estados e ActivityLog | Tasks 21, 22, 26, 27, 39 |
| RRULE, estratégias, clamp, skip past, EXDATE, count/until | Tasks 15, 23, 24, 28, 41 |
| Optimistic locking / HTTP 409 | Tasks 6, 19, 33, 40 |
| Soft delete, trash, restore e purge | Tasks 10, 29, 32, 37, 38 |
| Overdue | Tasks 21, 32, 38 |
| Kafka completed/reopened | Tasks 25–27, 41 |
| Formato padronizado de erros | Tasks 12, 17, 19–21, 33, 40 |
| Todos os endpoints REST | Tasks 34–39 |
| Unit, PostgreSQL, Kafka e REST Assured | Tasks 2–42 |

## Referências técnicas fixadas para este plano

- Quarkus 3.37.3 e criação de aplicações Kafka: https://quarkus.io/guides/kafka-getting-started
- Hibernate ORM with Panache repository pattern: https://quarkus.io/guides/hibernate-orm-panache
- Dev Services PostgreSQL/Kafka baseados em containers: https://quarkus.io/guides/dev-services
- Flyway Quarkus: https://quarkus.io/extensions/io.quarkus/quarkus-flyway/
- SmallRye Fault Tolerance: https://quarkus.io/guides/smallrye-fault-tolerance
- Quarkus `@InjectMock` e `quarkus-junit-mockito`: https://quarkus.io/guides/getting-started-testing
- ical4j `Recur<T>` e `getNextDate`: https://ical4j.github.io/docs/ical4j/api/4.0.0-rc4/ical4j.core/net/fortuna/ical4j/model/Recur.html
- Artefato ical4j 4.2.5: https://central.sonatype.com/artifact/org.mnode.ical4j/ical4j
- Artefato jsoup 1.22.2: https://central.sonatype.com/artifact/org.jsoup/jsoup
