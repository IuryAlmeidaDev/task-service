# Task Service — Especificação de Design

## Escopo

Microsserviço de gerenciamento de tarefas para a plataforma LifeOS. Responsável por captura
rápida (inbox), organização por projetos e áreas, priorização temporal, recorrência e emissão
de eventos Kafka consumidos pelo `finance-service`.

- **Identidade:** `groupId=dev.iury.lifeos`, `artifactId=task-service`, pacote raiz
  `dev.iury.lifeos.task`, porta HTTP `8081`.
- **Stack:** Java 21, Quarkus 3.37.3, Hibernate ORM with Panache, PostgreSQL, Flyway,
  SmallRye Reactive Messaging Kafka, Quarkus REST Jackson, Hibernate Validator, Quarkus
  Scheduler.
- **Banco isolado:** `task_db`; migrations via Flyway são a única forma de alterar o schema.

## Filosofia

Inspirado em Things 3 (foco temporal e clareza), Todoist (P1–P4 e captura rápida) e TickTick
(Won't Do / Cancelled). A prioridade real é **temporal** — o que importa é o que vai ser feito
**hoje**, não um número abstrato. Prioridade numérica (P1–P3) existe como refinamento dentro
do dia, não como sistema principal.

**Princípio Anti-Overengineering:** subtarefas em 1 nível apenas, sem dependências no MVP,
sem gamificação no MVP, sem Gantt/CPM. O sistema deve funcionar com zero configuração — Inbox
+ marcar como feito cobre 80% do uso.

## Modelo de Domínio

### Entidades

#### Area

Agrupamento não-completável que representa uma responsabilidade permanente do usuário
(ex: Trabalho, Casa, Saúde, Estudos). Não possui data de conclusão nem checkbox.

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| user_id | UUID | NOT NULL |
| name | VARCHAR(100) | NOT NULL |
| color | VARCHAR(7) | Hex color, nullable |
| sort_order | INTEGER | NOT NULL, default 0 |
| is_active | BOOLEAN | NOT NULL, default true |
| created_at | TIMESTAMP | NOT NULL, UTC |

#### Project

Container completável que representa um objetivo finito com definição clara de "pronto"
(ex: "Organizar viagem para SP", "Redesign do site").

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| user_id | UUID | NOT NULL |
| name | VARCHAR(200) | NOT NULL |
| description | TEXT | nullable |
| area_id | UUID | FK → Area, nullable |
| status | VARCHAR(20) | NOT NULL: ACTIVE, COMPLETED, ARCHIVED |
| due_date | DATE | nullable |
| color | VARCHAR(7) | Hex color, nullable |
| sort_order | INTEGER | NOT NULL, default 0 |
| created_at | TIMESTAMP | NOT NULL, UTC |
| updated_at | TIMESTAMP | NOT NULL, UTC |

#### Task

Ação concreta e executável. Unidade central do sistema.

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| user_id | UUID | NOT NULL |
| title | VARCHAR(500) | NOT NULL |
| description | TEXT | nullable |
| status | VARCHAR(20) | NOT NULL, default INBOX |
| priority | VARCHAR(10) | NOT NULL, default NONE |
| scheduled_date | DATE | nullable, "do date" — quando trabalhar |
| due_date | DATE | nullable, "deadline" — prazo externo |
| completed_at | TIMESTAMP | nullable, Instant UTC |
| cancelled_at | TIMESTAMP | nullable, Instant UTC |
| deleted_at | TIMESTAMP | nullable, soft-delete |
| expected_amount | NUMERIC(19,2) | nullable, integração financeira |
| project_id | UUID | FK → Project, nullable |
| area_id | UUID | FK → Area, nullable |
| parent_task_id | UUID | FK → Task (self), nullable |
| recurring_rule_id | UUID | FK → TaskRecurringRule, nullable |
| source_event_type | VARCHAR(50) | nullable, origem de evento externo |
| source_event_id | UUID | nullable, ID do evento de origem |
| sort_order | INTEGER | NOT NULL, default 0 |
| created_at | TIMESTAMP | NOT NULL, UTC |
| updated_at | TIMESTAMP | NOT NULL, UTC |

Índices: `UNIQUE(source_event_type, source_event_id) WHERE source_event_type IS NOT NULL`.

#### TaskRecurringRule

Define padrão de repetição de uma tarefa.

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| user_id | UUID | NOT NULL |
| frequency | VARCHAR(20) | NOT NULL |
| mode | VARCHAR(20) | NOT NULL |
| interval_value | INTEGER | Para CUSTOM, intervalo em dias |
| day_of_week | VARCHAR(9) | Para WEEKLY/BIWEEKLY |
| day_of_month | INTEGER | Para MONTHLY (1–31) |
| start_date | DATE | NOT NULL |
| end_date | DATE | nullable |
| is_active | BOOLEAN | NOT NULL, default true |
| last_generated_date | DATE | nullable |
| created_at | TIMESTAMP | NOT NULL, UTC |

#### Tag

Etiqueta transversal para contexto (ex: `#trabalho`, `#casa`, `#urgente`, `#esperando`).

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| user_id | UUID | NOT NULL |
| name | VARCHAR(50) | NOT NULL |
| color | VARCHAR(7) | Hex color, nullable |

`UNIQUE(user_id, name)`.

#### TaskTag (Join Table)

| Campo | Tipo | Restrições |
|-------|------|------------|
| task_id | UUID | FK → Task, PK |
| tag_id | UUID | FK → Tag, PK |

#### ChecklistItem

Micro-passos leves dentro de uma tarefa. Não possuem data, prioridade ou estado próprio —
são apenas checkboxes.

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| task_id | UUID | FK → Task, NOT NULL |
| title | VARCHAR(300) | NOT NULL |
| is_checked | BOOLEAN | NOT NULL, default false |
| sort_order | INTEGER | NOT NULL, default 0 |

#### Outbox

Tabela de outbox para garantia de publicação Kafka via Transactional Outbox Pattern.

| Campo | Tipo | Restrições |
|-------|------|------------|
| id | UUID | PK, auto-generated |
| aggregate_type | VARCHAR(50) | NOT NULL (ex: "Task") |
| aggregate_id | UUID | NOT NULL |
| event_type | VARCHAR(50) | NOT NULL |
| payload | JSONB | NOT NULL |
| created_at | TIMESTAMP | NOT NULL, UTC |
| published_at | TIMESTAMP | nullable |

### Enumerações

| Enum | Valores |
|------|---------|
| **TaskStatus** | `INBOX`, `ACTIVE`, `COMPLETED`, `CANCELLED` |
| **Priority** | `P1` (urgente), `P2` (alta), `P3` (média), `NONE` (padrão) |
| **ProjectStatus** | `ACTIVE`, `COMPLETED`, `ARCHIVED` |
| **RecurringFrequency** | `DAILY`, `WEEKDAYS`, `WEEKLY`, `BIWEEKLY`, `MONTHLY`, `YEARLY`, `CUSTOM` |
| **RecurrenceMode** | `CALENDAR_BASED`, `COMPLETION_BASED` |

### Diagrama de Relacionamentos

```
Area (1) ◄───── (N) Project (1) ◄───── (N) Task (1) ◄───── (N) ChecklistItem
  ▲                                          │  ▲
  │                                          │  │ parent_task_id
  └──────────────────── (N) Task ◄───────────┘  │ (self-ref, máx 1 nível)
                              │
                              ├──── (N) TaskTag ────► (1) Tag
                              │
                              └──── (N..1) TaskRecurringRule
```

## Regras de Negócio Críticas

### Ciclo de Vida da Tarefa

1. **Status padrão é INBOX.** Toda tarefa criada sem projeto, área ou data entra como INBOX.
2. **Saída do INBOX:** atribuir `scheduledDate`, `dueDate`, `project` ou `area` move
   automaticamente para ACTIVE. Também pode ser movida manualmente.
3. **Transições válidas:**
   - `INBOX → ACTIVE` (triagem)
   - `ACTIVE → COMPLETED` (conclusão)
   - `ACTIVE → CANCELLED` (desistência / Won't Do)
   - `INBOX → COMPLETED` (conclusão direta do inbox)
   - `INBOX → CANCELLED` (desistência direta)
   - `COMPLETED → ACTIVE` (reabertura)
   - `CANCELLED → ACTIVE` (reativação)
4. **Qualquer → COMPLETED** preenche `completedAt = now()` e emite `TASK_COMPLETED` via Outbox.
5. **COMPLETED → ACTIVE** limpa `completedAt` e emite `TASK_REOPENED` via Outbox.
6. **CANCELLED ≠ COMPLETED.** Cancelamento não conta como conclusão para métricas e **não**
   emite evento Kafka.
7. **Completar tarefa-pai** completa automaticamente todas as subtarefas INBOX/ACTIVE.
8. **Deletar tarefa** é soft-delete (`deleted_at = now()`). Tarefas deletadas não aparecem
   em nenhuma view e não emitem eventos.

### Prioridade e Ordenação

9. **Prioridade padrão é NONE.**
10. **Ordenação nas views:** `scheduled_date ASC NULLS LAST` → `priority ASC` → `due_date ASC
    NULLS LAST` → `sort_order ASC` → `created_at ASC`.
11. **View "Today":** tarefas ACTIVE com `scheduled_date ≤ hoje` OU `due_date ≤ hoje`. Tarefas
    atrasadas ficam no topo.
12. **View "Upcoming":** tarefas ACTIVE com `scheduled_date > hoje`, agrupadas por data.
13. **View "Someday":** tarefas ACTIVE sem `scheduled_date` e sem `due_date`.
14. **View "Inbox":** tarefas com `status = INBOX`.

### Subtarefas

15. **Máximo 1 nível de aninhamento.** Criar subtarefa de subtarefa retorna erro 400.
16. **Subtarefas herdam** `project` e `area` do pai se não explicitamente definidos.
17. **Subtarefas possuem** datas e prioridade independentes.
18. **Reabrir tarefa-pai** NÃO reabre subtarefas automaticamente.

### Recorrência

19. **Calendar-Based:** próxima ocorrência é a próxima data do calendário que satisfaz a regra.
20. **Completion-Based:** próxima ocorrência = `completedAt + intervalo`.
21. **Completar tarefa recorrente** cria uma NOVA instância com a próxima data calculada. A
    instância original permanece COMPLETED no histórico.
22. **Tarefa recorrente atrasada (Calendar-Based):** ao completar, a próxima ocorrência pula
    para a próxima data futura. Não gera backlog de ocorrências perdidas.
23. **Cancelar instância recorrente** pula para a próxima ocorrência sem contar como concluída.
24. **Regra com `end_date`:** a última ocorrência em `end_date` é gerada; após isso, a regra é
    desativada (`is_active = false`).

### Projetos e Áreas

25. **Completar projeto** NÃO completa automaticamente suas tarefas pendentes. Requer ação
    explícita do usuário.
26. **Arquivar projeto** move-o para o arquivo. Tarefas pendentes do projeto permanecem ativas.
27. **Area nunca é completada.** Tentativa retorna erro 400.

### Tags

28. **Nome de tag é único por usuário.** `UNIQUE(user_id, name)`.
29. **Deletar tag** remove associações `TaskTag` mas não altera as tarefas.

### Datas

30. **`scheduled_date` (Do Date):** quando o usuário pretende trabalhar na tarefa. Determina
    visibilidade nas views Today/Upcoming.
31. **`due_date` (Deadline):** prazo externo e inflexível. Tarefas com deadline próximo
    (≤ 3 dias) recebem indicador visual na API.

### Kafka (Outbox Pattern)

32. **Toda transição para COMPLETED** insere atomicamente um registro na tabela `outbox`.
33. **Toda reabertura (COMPLETED → ACTIVE)** insere atomicamente um registro na tabela `outbox`.
34. **Um scheduler** lê registros não-publicados da `outbox` e publica no Kafka. Após
    publicação, marca `published_at`.
35. **Serialização temporal:** `Instant` serializado como ISO-8601 UTC (`yyyy-MM-dd'T'HH:mm:ss'Z'`).
36. **Idempotência de consumo (food events):** `UNIQUE(source_event_type, source_event_id)`
    previne criação duplicada de tarefas a partir de eventos reprocessados.

## Contratos Kafka

### Produzidos pelo Task Service

**ATENÇÃO:** Estes contratos são FIXOS e já consumidos pelo `finance-service` em produção.
Nenhum campo pode ser renomeado, adicionado ou removido.

#### TASK_COMPLETED (tópico: `task.completed.events`)

```json
{
  "eventType": "TASK_COMPLETED",
  "taskId": "uuid",
  "title": "Pagar conta de luz",
  "expectedAmount": 150.00,
  "completedAt": "2026-07-22T15:00:00Z",
  "timestamp": "2026-07-22T15:00:01Z"
}
```

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| eventType | String | Sim | Literalmente `"TASK_COMPLETED"` |
| taskId | UUID | Sim | ID da tarefa |
| title | String | Sim | Título da tarefa |
| expectedAmount | BigDecimal | Não | Valor financeiro. Se null, finance-service usa `0.00` |
| completedAt | Instant | Sim | Quando o usuário marcou como concluída |
| timestamp | Instant | Sim | Quando o evento foi publicado |

#### TASK_REOPENED (tópico: `task.reopened.events`)

```json
{
  "eventType": "TASK_REOPENED",
  "taskId": "uuid",
  "reopenedAt": "2026-07-22T16:00:00Z",
  "timestamp": "2026-07-22T16:00:01Z"
}
```

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| eventType | String | Sim | Literalmente `"TASK_REOPENED"` |
| taskId | UUID | Sim | ID da tarefa |
| reopenedAt | Instant | Sim | Quando a tarefa foi reaberta |
| timestamp | Instant | Sim | Quando o evento foi publicado |

### Consumidos pelo Task Service

#### SHOPPING_LIST_CREATED (tópico: `food.shopping-list.events`)

```json
{
  "eventType": "SHOPPING_LIST_CREATED",
  "shoppingListId": "uuid",
  "userId": "uuid",
  "weekDate": "2026-09-08",
  "totalEstimatedCost": 250.00,
  "suggestedShoppingDate": "2026-09-13",
  "timestamp": "2026-09-08T10:00:00Z"
}
```

Ao receber, cria automaticamente uma tarefa:

- `title`: `"🛒 Fazer feira da semana"`
- `scheduled_date`: `suggestedShoppingDate`
- `expected_amount`: `totalEstimatedCost`
- `status`: `ACTIVE`
- `source_event_type`: `"SHOPPING_LIST_CREATED"`
- `source_event_id`: `shoppingListId`
- Tag automática `#compras` (criada se não existir)

## Pontos de Integração

| Direção | De → Para | Mecanismo | Status |
|---------|-----------|-----------|--------|
| Task → Finance | task → finance | Kafka: `task.completed.events` | **MVP** (contrato fixo) |
| Task → Finance | task → finance | Kafka: `task.reopened.events` | **MVP** (contrato fixo) |
| Food → Task | food → task | Kafka: `food.shopping-list.events` | **MVP** |
| Finance → Task | finance → task | Kafka: `finance.bill.due.events` | **V2** — vencimentos geram lembretes |

### Fluxo Completo: Food → Task → Finance

```
[Food Service]                [Task Service]              [Finance Service]
     │                              │                              │
     │ SHOPPING_LIST_CREATED        │                              │
     │ ──────────────────────────►  │                              │
     │                              │ cria Task "Fazer feira"      │
     │                              │ scheduledDate = sábado       │
     │                              │ expectedAmount = R$250       │
     │                              │                              │
     │                              │ (usuário faz compras e       │
     │                              │  marca tarefa COMPLETED)     │
     │                              │                              │
     │                              │ TASK_COMPLETED               │
     │                              │ ──────────────────────────►  │
     │                              │                              │ cria Transaction
     │                              │                              │ type = EXPENSE
     │                              │                              │ amount = R$250
     │                              │                              │ status = PENDING
```

## Superfície REST API

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| **Tarefas** | | |
| GET | `/api/v1/tasks` | Listar (filtros: status, project, area, tag, date range, priority) |
| GET | `/api/v1/tasks/today` | View Today |
| GET | `/api/v1/tasks/upcoming` | View Upcoming (agrupado por data) |
| GET | `/api/v1/tasks/inbox` | View Inbox |
| POST | `/api/v1/tasks` | Criar tarefa (quick capture) |
| GET | `/api/v1/tasks/{id}` | Detalhe |
| PUT | `/api/v1/tasks/{id}` | Atualizar |
| POST | `/api/v1/tasks/{id}/complete` | Completar |
| POST | `/api/v1/tasks/{id}/reopen` | Reabrir |
| POST | `/api/v1/tasks/{id}/cancel` | Cancelar |
| DELETE | `/api/v1/tasks/{id}` | Soft-delete |
| **Subtarefas** | | |
| GET | `/api/v1/tasks/{id}/subtasks` | Listar subtarefas |
| POST | `/api/v1/tasks/{id}/subtasks` | Criar subtarefa |
| **Checklist** | | |
| GET | `/api/v1/tasks/{id}/checklist` | Listar items |
| POST | `/api/v1/tasks/{id}/checklist` | Adicionar item |
| PUT | `/api/v1/tasks/{taskId}/checklist/{itemId}` | Atualizar (toggle, reordenar) |
| DELETE | `/api/v1/tasks/{taskId}/checklist/{itemId}` | Remover |
| **Projetos** | | |
| GET/POST | `/api/v1/projects` | Listar / criar |
| GET/PUT/DELETE | `/api/v1/projects/{id}` | Detalhe / atualizar / arquivar |
| POST | `/api/v1/projects/{id}/complete` | Completar projeto |
| **Áreas** | | |
| GET/POST | `/api/v1/areas` | Listar / criar |
| GET/PUT/DELETE | `/api/v1/areas/{id}` | Detalhe / atualizar / desativar |
| **Tags** | | |
| GET/POST | `/api/v1/tags` | Listar / criar |
| GET/PUT/DELETE | `/api/v1/tags/{id}` | Detalhe / atualizar / deletar |

## MVP vs Nice-to-Have

### MVP (Entrega 1)

- Task CRUD com ciclo de vida completo (INBOX → ACTIVE → COMPLETED / CANCELLED)
- Inbox para captura rápida
- Projects e Areas
- Tags
- Prioridade P1–P3 + NONE
- Scheduled Date + Due Date (separados, estilo Things 3)
- Subtarefas (1 nível, first-class)
- Checklist inline (lightweight)
- Recorrência dual-engine: Calendar-Based + Completion-Based
  (DAILY, WEEKDAYS, WEEKLY, BIWEEKLY, MONTHLY, YEARLY, CUSTOM)
- Eventos Kafka via Outbox: `TASK_COMPLETED`, `TASK_REOPENED`
- Consumo de `SHOPPING_LIST_CREATED` do food-service
- Views: Today, Upcoming, Inbox, Someday
- REST API completa com paginação

### Nice-to-Have (Entrega 2+)

- Dependências entre tarefas (blocked/blocking com detecção de ciclo DFS)
- Gamificação: streaks com freeze, meta diária, celebração "Today cleared!"
- Eisenhower Matrix como view derivada
- Smart Filters / consultas salvas
- Estimativa de tempo por tarefa
- Consumo de `finance.bill.due.events` para lembretes de pagamento automáticos
- Recorrências avançadas (nth weekday do mês)
- Anexos em tarefas
- Busca full-text (PostgreSQL tsvector)

## Anti-patterns a Evitar

1. **Subtarefas profundas (>1 nível):** Tarefas com 3+ níveis de decomposição são projetos,
   não tarefas. Subtarefas profundas desaparecem das views diárias e são esquecidas.
2. **Priority Inflation:** P1–P3 sem disciplina resulta em 70%+ das tarefas como P1. O foco
   temporal (Today) é mais eficaz que números.
3. **Recorrência com backlog:** Tarefas recorrentes atrasadas que geram N instâncias perdidas
   criam avalanche de culpa. Sempre pular para a próxima data futura.
4. **Gamificação punitiva:** Reduzir pontos por atraso causa ansiedade e abandono. Usar apenas
   reforço positivo (quando implementado).
5. **Inbox como lista permanente:** O Inbox deve incentivar triagem rápida, não ser usado como
   lista principal de trabalho.
6. **Kanban para produtividade pessoal:** Colunas To Do/Doing/Done adicionam cerimônia sem
   valor para uma pessoa só. Views temporais (Today/Upcoming) são mais eficazes.
7. **Due Date como "Do Date":** Confundir prazo externo com data de execução planejada gera
   ruído visual e destrói a confiança nas datas. Manter os dois campos separados.
