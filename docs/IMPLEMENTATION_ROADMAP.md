# Task Service — Roadmap de Implementação

Este roadmap divide o plano de 42 tasks em blocos executáveis em sessões separadas, mantendo a ordem das dependências e evitando refazer trabalho já integrado.

## Fonte técnica

O detalhamento de arquivos, testes e comandos está em `docs/superpowers/plans/2026-07-22-task-service.md`. Este documento define apenas prioridade e agrupamento.

## Ordem de prioridades

### Parte 0 — Bootstrap e validação do ambiente

Implementar ou validar a Task 1: Quarkus, Maven Wrapper, Java 21, configuração de banco e Dev Services. Antes de continuar, executar os testes da fundação.

### Parte 1 — Banco e modelo persistente

Implementar Tasks 2–7:

- migrations das tabelas de referência;
- migrations de tarefas, relações e auditoria;
- entidades `Project` e `Tag`;
- entidades de recorrência;
- entidade `Task` e enums;
- entidades de associação, dependência e auditoria.

### Parte 2 — Persistência e regras básicas

Implementar Tasks 8–15:

- repositories de project e tag;
- repositories de recorrência;
- `TaskRepository` e filtros;
- repositories de relações e auditoria;
- validação e sanitização;
- `ProjectService` e Inbox;
- `TagService`;
- parser e persistência de RRULE.

### Parte 3 — Núcleo funcional de tarefas

Implementar Tasks 16–24:

- criação de tarefas comuns e recorrentes;
- regras de subtarefas;
- consulta, filtros e detalhe;
- atualização com optimistic locking;
- dependências e detecção de ciclos;
- máquina de estados;
- transições, bloqueio e auditoria;
- cálculo de próxima ocorrência;
- clonagem e limites de séries recorrentes.

### Parte 4 — Eventos, ciclo de vida e jobs

Implementar Tasks 25–32:

- contratos e publisher Kafka;
- conclusão transacional;
- reabertura com evento compensatório;
- pular ocorrência recorrente;
- soft delete e restauração em cascata;
- exclusão de projeto movendo tarefas para Inbox;
- associação de tags;
- jobs de overdue e purge.

### Parte 5 — API REST

Implementar Tasks 33–40:

- formato padronizado de erros;
- REST de Projects;
- REST de Tags e associação com Task;
- DTOs e mapper;
- CRUD, filtros e paginação de Task;
- ações, lixeira e overdue;
- dependências e histórico;
- cenários REST de erro, concorrência e filtros combinados.

### Parte 6 — Aceite e entrega

Implementar Tasks 41–42:

- integração real com PostgreSQL, Kafka e recorrência;
- documentação e verificação final.

## Regra de execução

Executar uma Task por vez, seguindo o ciclo TDD do plano: teste falho, RED, implementação mínima, GREEN e commit próprio. Não avançar para a próxima Parte enquanto a anterior não estiver validada.

Antes de cada sessão, verificar `git status` e os commits existentes. Se uma Task já estiver implementada, validar o comportamento e seguir para a próxima sem duplicar código.

## Próximo passo

Começar pela Parte 0, validando a Task 1 com JDK 21.
