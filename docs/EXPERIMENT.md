# Воспроизведение экспериментов ReliabilityLab

Адаптер получает заказы из RabbitMQ, передаёт их в API Элемента и фиксирует решения наблюдателя. Исходный генератор использован из репозитория Лины. Для публикации используются долговечные очереди, persistent-сообщения и publisher confirms. Если заказ подтверждён ответом обработки или чтением состояния, потребитель отправляет ACK. После исчерпания лимита выполняется reject. RabbitMQ переносит доставку в очередь `.unresolved` с подтверждением получателя. Для этих настроек нужен RabbitMQ 4.2 с quorum queues. [Документация RabbitMQ](https://www.rabbitmq.com/docs/4.2/quorum-queues)

## Запуск

Установите Python 3.10+ и зависимости. Поднимите отдельный тестовый RabbitMQ, опубликовав AMQP-порт только на loopback. `RELIABILITYLAB_AMQP_URL` указывает на этот брокер. API доступен по HTTPS. Для локального запуска задайте `RELIABILITYLAB_API_TOKEN` или пару `RELIABILITYLAB_CLIENT_ID`/`RELIABILITYLAB_CLIENT_SECRET` приложения. Порядок выдачи, права и URL описаны в [ELEMENT_HANDOFF.md](ELEMENT_HANDOFF.md).

```sh
make setup
git clone https://github.com/Linasthsmb/ReliabilityLab.git .local/lina
git -C .local/lina checkout --detach 0b75d485541f82e80746a0bad66188b297f891ae
export RELIABILITYLAB_API_BASE_URL='https://app-802542381.1cmycloud.com/applications/Reliability-lab-dev/api/reliability/v1'
# Задайте RELIABILITYLAB_AMQP_URL и учётные данные Элемента в локальном окружении.
generator_rev=$(git -C .local/lina rev-parse HEAD)
runner_rev=$(git rev-parse HEAD)
python=.venv/bin/python
for seed in 101 102 103; do
  for strategy in A B C; do
    "$python" -m reliabilitylab.runner .local/lina/python \
      --strategy "$strategy" --scenario drop_response_after_commit \
      --amount-unit kopecks --count 100 --seed "$seed" \
      --broker-version 4.2.9 --element-version 1.0-13 \
      --generator-commit "$generator_rev" --runner-commit "$runner_rev" \
      --output .local/paper-runs || exit 1
  done
done
"$python" -m pip install -r requirements-report.txt
"$python" -m reliabilitylab.report .local/paper-runs \
  --scenario drop_response_after_commit --output .local/paper-report
```

Подставьте фактические версии сервера и приложения. Единицы `amount` выбираются явно. В сохранённом эксперименте целые значения генератора трактуются как синтетические копейки. Для каждого прогона создаются новый Run ID, две собственные очереди и отдельные файлы. Прогоны изменяют данные dev и сохраняются для сверки.

Если процесс прервался после публикации, дождитесь отключения старого потребителя и обработайте оставшиеся доставки с тем же `manifest.json`. Убедитесь, что старый процесс завершился. Команда не публикует сообщения повторно. Сохраните отдельный журнал этого запуска для разбора, а прерванный прогон исключите из автоматического сравнения.

```sh
.venv/bin/python -m reliabilitylab.worker .local/paper-runs/<run-id>/manifest.json \
  --journal .local/recovered-events.jsonl
```

## Методика и результаты

- `normal` проверяет обычный путь. `fail_before_commit` вызывает документированный откат первой бизнес-попытки. `duplicate_delivery` публикует вторую копию с тем же Operation ID. Повторная доставка самим брокером после закрытия канала была проверена отдельно.
- В `drop_response_after_commit` потребитель получает успешный ответ, отбрасывает его до принятия решения и фиксирует `UNKNOWN`. A повторяет команду, B сначала читает состояние, C повторяет команду с защитой на сервере. Этот сценарий моделирует потерю ответа после подтверждённого сохранения заказа. Реальный сетевой разрыв и гонка B требуют других условий. Устаревший результат GET в B проверен прямой приёмкой API.
- В каждой серии seed фиксирует одинаковые бизнес-поля между A/B/C. В журнале различаются подтверждение публикации, приём HTTP, подтверждение заказа и ACK. По завершении отдельный GET для каждой операции возвращает фактические ссылки на заказы. Суммы сверяются с агрегатами Элемента.
- `input.jsonl` содержит генерируемые данные, `events.jsonl` — клиентские события, `orders.csv` — независимую сверку документов, `result.json` — метрики. График строится по завершённым сопоставимым сериям. Отчёт рассчитывает p95 методом ближайшего ранга при наличии не менее 100 наблюдений в серии. Интервалы измеряются монотонными часами клиента. Сверка заказов после завершения в клиентские задержки не входит.
- Проверочный режим рассчитан на одно развёртывание Элемента и один RabbitMQ. Зафиксированные результаты описывают эти условия и синтетические входы.

Локальные тесты запускаются командой `.venv/bin/python -m unittest discover -s tests -p test_adapter.py -v`. Сквозные проверки опубликованного API выполняются отдельно через `make test-live`.
