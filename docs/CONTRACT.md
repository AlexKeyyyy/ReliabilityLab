# Контракт ReliabilityLab HTTP API — проект v1

Статус: целевой проект API; первый срез C опубликован и проверяется на runtime 10.0.7-28. Точный текущий набор маршрутов, DTO, авторизация, коды ошибок и ограничения — в [FIRST_SLICE.md](FIRST_SLICE.md). A/B, метрики и observations в этом документе остаются проектом; контракт пока не согласован с Линой. Бизнес-алгоритмы и модель — [ARCHITECTURE.md](ARCHITECTURE.md).

## Адрес и доступ

Метаданные HttpСервис: `ReliabilityLabApi`, `КорневойUrl: /reliability/v1`. Платформа добавляет `/api/`: полный адрес `{APP_PUBLICATION_URL}/api/reliability/v1`. Фактический адрес приложения ещё неизвестен. [Формат URL в документации](https://1cmycloud.com/console/help/element/10.0/docs/topics/http-service-element/).

Штатная аутентификация приложения: служебный пользователь с доступом к HTTP-сервису, отдельные ClientId/ClientSecret → POST `{APP_PUBLICATION_URL}/sys/token` с `grant_type=CLIENT_CREDENTIALS` → поле `id_token` → `Authorization: Bearer ...`. Эти ключи не равны логину/паролю панели и не должны автоматически переиспользовать её credentials. Токен допускает повторные вызовы; при 401 клиент обновляет его ограниченно, не повторяя неопределённую бизнес-операцию вслепую. [Документация](https://1cmycloud.com/console/help/element/10.0/docs/topics/external-program-authentication/).

Все тела JSON, `Content-Type: application/json`; ошибки не раскрывают stack trace и секреты. Управление baseline и планом сбоев доступно владельцу эксперимента, обычный процессинг — служебному пользователю. Конкретные права проверяются при публикации.

## Бизнес-сообщение

```json
{
  "schema_version": 1,
  "operation_id": "run001_op000001",
  "order_id": "ORD-001",
  "run_id": "run001",
  "attempt_id": "c298f5d0-03ca-4c78-b4cb-b699781d9872",
  "amount_minor": 150000,
  "currency": "RUB",
  "created_at": "2026-11-20T10:00:00Z"
}
```

Проектные правила v1:

- ID операции, заказа и прогона: непустые ASCII-строки `[A-Za-z0-9_-]{1,96}`. OperationId глобален; RunId неизменяем. AttemptId — UUID v4 новой HTTP-попытки, глобально уникальный.
- `amount_minor`: целое число от 1 до 999999999999; сумма в минимальных единицах, без float. Для MVP только синтетические RUB. В исходной спецификации было поле `amount`: здесь единицы сделаны явными; это изменение нужно передать Лине до реализации генератора.
- `created_at`: RFC 3339, UTC с `Z`, точность до миллисекунд включительно. Сервер приводит к единому значению момента; собственные времена записывает независимо.
- schema_version строго 1, неизвестные поля отклоняются; лимит тела 16 KiB — прикладная проверка при реализации.
- Неизменяемое бизнес-содержимое включает все поля кроме AttemptId. Одинаковый OperationId с другим содержимым — 409.
- Сценарий, стратегия и fault plan задаются прогоном. Их нельзя менять отдельным повторным вызовом.

## Маршруты

Все пути ниже относительно `/api/reliability/v1`.

| Метод / путь | Вход | Успех и смысл |
|---|---|---|
| POST /runs | run_id, strategy A/B/C, scenario, seed, dataset_id, fault_plan_id, expected_count, baseline_enabled, config | 201 новый прогон; 200 точный повтор конфигурации; 409 изменение существующей |
| POST /messages | Бизнес-сообщение выше | 201 inbox устойчиво создан; 200 уже существует с тем же содержимым. Заказ этим вызовом не создаётся |
| POST /operations/{id}/process | attempt_id | 200 после commit заказа или восстановления результата. Для A/B новая попытка может создать новый эффект |
| POST /operations/{id}/retry | attempt_id | Тот же бизнес-метод и правила, что process; отдельный маршрут для явного восстановления и журнала |
| GET /operations/{id} | — | 200: серверное состояние, список фактических заказов, последний исход попытки; 404 неизвестная операция |
| GET /operations/{id}/events | limit, cursor | 200: страницы событий и следующий cursor |
| GET /runs/{id}/operations | limit, cursor, status | 200: страницы операций, стабильный порядок OperationId |
| GET /runs/{id}/metrics | — | 200: серверные агрегаты, клиентские показатели при наличии наблюдений |
| POST /runs/{id}/observations | batch клиентских наблюдений | 200 после записи batch, повторы EventId безопасны; не меняет бизнес-состояние |

Сначала реализуются runs/messages/process/GET operation. Retry — тонкий alias общего метода, остальные чтения и observations добавляются с UI. В MVP не предусматривается публичный DELETE для очистки прогонов.

Пагинация: limit по умолчанию 100, максимум 500; непрозрачный cursor. Новые события имеют собственный EventId, а не ключ только по времени.

## Успешные ответы

Ответ приёма содержит `schema_version`, `operation_id`, `run_id`, `server_state`, `durable_stage: "INBOX_PERSISTED"`, `received_at`, `replayed`. Он не даёт оснований для ACK выбранного первого MVP.

Пример ответа обработки C:

```json
{
  "schema_version": 1,
  "operation_id": "run001_op000001",
  "run_id": "run001",
  "attempt_id": "9156036d-4f46-4e41-a5dd-7736725c0c26",
  "server_state": "COMPLETED",
  "durable_stage": "BUSINESS_COMMITTED",
  "replayed": true,
  "result": {
    "order_ref": "<фактический UUID документа>",
    "order_id": "ORD-001",
    "amount_minor": 150000,
    "currency": "RUB",
    "completed_at": "2026-11-20T10:00:01Z"
  }
}
```

`result` C неизменен между попытками. Оболочка содержит ID текущей попытки и replay-признак. Для A/B каждый новый эффект имеет отдельный order_ref; GET возвращает все заказы, а не скрывает лишние за единственной ссылкой. Число бизнес-эффектов вычисляется по заказам.

`GET operation` возвращает отдельно `server_state`, `last_attempt_outcome`, `orders[]`, `business_effect_count`; при наличии клиентских данных — `observer_state`. UNKNOWN не заменяет серверный COMPLETED. GET не запускает обработку и не создаёт заказ.

Повтор завершённого AttemptId возвращает сохранённый ответ без новой попытки. Повтор STARTED — 409 ATTEMPT_IN_PROGRESS: клиент выясняет результат операции и при необходимости создаёт новую попытку. До проверки состояния STARTED нельзя называть доказанной ошибкой.

## Ошибки

```json
{
  "schema_version": 1,
  "operation_id": "run001_op000001",
  "attempt_id": "c298f5d0-03ca-4c78-b4cb-b699781d9872",
  "error": {
    "code": "OPERATION_PAYLOAD_CONFLICT",
    "message": "Operation payload differs from the stored payload",
    "retryable": false
  }
}
```

| Код | Случаи |
|---|---|
| 400 | INVALID_JSON, VALIDATION_ERROR, UNSUPPORTED_SCHEMA_VERSION |
| 401/403 | Платформенная аутентификация/разрешения; формат может отличаться от нашей JSON-ошибки |
| 404 | RUN_NOT_FOUND, OPERATION_NOT_FOUND |
| 409 | OPERATION_PAYLOAD_CONFLICT, ATTEMPT_PAYLOAD_CONFLICT, ATTEMPT_IN_PROGRESS, RUN_CONFIG_CONFLICT, BASELINE_DISABLED |
| 413/415 | Слишком большое тело / неподдерживаемый тип содержимого; ответ шлюза тоже возможен |
| 503 | LOCK_CONFLICT, TEMPORARILY_UNAVAILABLE; ограниченный повтор после сверки состояния |
| 500 | INJECTED_BEFORE_COMMIT или INTERNAL_ERROR; нельзя выводить из HTTP-кода отсутствие заказа без сверки |

При неизвестном результате commit не возвращать выдуманный FAILED: запись могла сохраниться. Клиент сверяет фактический результат.

## Наблюдения и метрики

Наблюдение: `event_id` UUID, `operation_id`, `attempt_id` при наличии, `observer_id`, `sequence` (возрастающий номер в пределах observer/operation), `event_type`, `occurred_at`, детали. Source сервер назначает ADAPTER. Разрешённые типы: PUBLISHED_CONFIRMED, RESPONSE_DISCARDED, OUTCOME_UNKNOWN, OUTCOME_RESOLVED, ACK_SENT. Сервер проверяет принадлежность OperationId к RunId; события публикации до приёма разрешаются для зарегистрированного RunId и позже связываются с inbox. Batch до 100 событий, атомарно; повторы EventId с иным содержимым — 409. Предельный размер тела — 256 KiB.

Это проект дополнительных маршрутов, не реализация части Лины. Выгрузка её независимого журнала остаётся источником для перепроверки.

Sent — уникальные операции с подтверждённой публикацией по клиентскому журналу, Received — уникальный inbox, Completed — операции с хотя бы одним заказом, Duplicate Attempts — дополнительные process/retry попытки, Duplicate Business Effects — лишние заказы. Unknown — операции с последним клиентским исходом UNKNOWN без последующего разрешения по sequence; один observer на прогон. `observation_coverage` и `as_of` обязательны: отсутствие/неполнота наблюдений не превращается в ноль.

## Решения для согласования с Линой

1. Префикс `/api/reliability/v1`, имена полей и amount_minor вместо неопределённого amount.
2. Новые AttemptId для каждого HTTP-повтора; сравнимые бизнес-наборы и фиксированный fault plan.
3. ACK только после подтверждённого BUSINESS_COMMITTED, не после приёма inbox.
4. Основной сбой потери ответа моделируется адаптером и явно так называется.
5. Импорт клиентских наблюдений и окончательная схема raw events — перед UI/экспериментами.
