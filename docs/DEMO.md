# Демонстрация ReliabilityLab

Откройте Overview в приложении и итоговый [график](../experiments/comparison/2026-11-25/comparison.png). Индекс фиксирует Run ID всех девяти прогонов. Вставьте один Run ID A и один C в поле Overview по очереди, затем сравните число лишних заказов. [Индекс прогонов](../experiments/comparison/2026-11-25/index.json)

Для живой демонстрации выполните команды из [EXPERIMENT.md](EXPERIMENT.md) с `--count 1` для A, затем для C. Для обоих сохраните одинаковые seed и scenario `drop_response_after_commit`, оставив runner сгенерировать отдельный Run ID. Откройте Operations для нужного Operation ID, затем фильтруйте Orders и Events по этому же ID. В `events.jsonl` покажите `response_discarded`, `outcome_unknown`, `order_confirmed` и `consumer_ack`. Если соединение с dev недоступно, для разбора используйте сохранённые журналы и таблицу. [Архив фактических прогонов](../experiments/comparison/2026-11-25/raw-runs.tar.gz)
