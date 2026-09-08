# Task Service

Microsserviço de gerenciamento de tarefas da plataforma **LifeOS**.

## Stack

- Java 21
- Quarkus 3.37.3
- PostgreSQL (`task_db`)
- Apache Kafka (SmallRye Reactive Messaging)
- Flyway (migrations)

## Identidade

| Campo | Valor |
|-------|-------|
| groupId | `dev.iury.lifeos` |
| artifactId | `task-service` |
| Pacote raiz | `dev.iury.lifeos.task` |
| Porta HTTP | `8081` |

## Documentação

- [Design Spec](docs/superpowers/specs/2026-09-08-task-service-design.md)
