# Заключительный проект: Apache Flink + Kafka + Postgres (PyFlink)

Потоковый конвейер на **Apache Flink 1.20 (PyFlink)**: генератор IoT-событий → Kafka →
обогащение справочником из Postgres → оконная агрегация в **event time** (средняя
температура и **медиана** влажности по минутам) → Kafka. Весь стек поднимается
**одной командой** через Docker Compose.

## Архитектура

```
 generator (Python, confluent-kafka)
        │  раз в секунду, JSON: {type_id, event_time, temperature, humidity}
        ▼
 ┌──────────────┐        ┌───────────────────────────── Flink job (PyFlink) ─────────────────────────────┐
 │ kafka         │        │  src: kafka  (Table API / SQL DDL, WATERMARK = event time)                    │
 │ iot-events    ├───────►│        │                                                                      │
 └──────────────┘        │        ▼   LEFT JOIN ... FOR SYSTEM_TIME AS OF (lookup join со справочником)    │
 ┌──────────────┐        │  src: pg     (Table API / JDBC source)                                         │
 │ postgres      ├───────►│        │                                                                      │
 │ device_types  │        │        ▼   to_data_stream()      ← переход Table → DataStream                  │
 └──────────────┘        │   event-time tumbling window (1 мин): AVG(temp), MEDIAN(humidity)              │
                         │        │                                                                       │
                         │        ▼   from_data_stream()    ← переход DataStream → Table                  │
 ┌──────────────┐        │  sink: kafka (Table API / SQL DDL, JSON)                                       │
 │ kafka         │◄───────┤                                                                               │
 │ iot-aggregates│        └───────────────────────────────────────────────────────────────────────────┘
 └──────────────┘
        результат: {window_time "hh:mm", type_name (из pg), avg_temperature, median_humidity}
```

## Как запустить

Нужен **Docker** и **Docker Compose v2**. Рекомендуется выделить Docker ~6–8 ГБ RAM.

```bash
docker compose up --build
# или, если есть make:
make up
```

Первый запуск собирает образ Flink с PyFlink и качает коннекторы — это занимает
несколько минут. Дальше:

- **Flink Web UI** — http://localhost:8081 (там видно запущенную джобу)
- **Kafka UI** — http://localhost:8080 (топики и сообщения)

Порядок старта выстроен через healthcheck'и и `depends_on`:
`kafka` → `kafka-init` (создаёт топики) и `postgres` (грузит `ddl.sql`+`dml.sql`) →
`jobmanager`/`taskmanager` → `generator` и `job-submitter` (сабмитит джобу).

## Проверка результата

Входные события:
```bash
make events
# или:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic iot-events --from-beginning
```

Результат (появляется через ~1 минуту event-time после старта генератора):
```bash
make aggregates
# или:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic iot-aggregates --from-beginning
```

Пример выходного сообщения:
```json
{"window_time":"12:34","type_name":"Thermostat","avg_temperature":20.13,"median_humidity":45.2}
```

Справочник в Postgres:
```bash
make pg-check
```

## Где какое требование задания реализовано

| Требование из задания | Где в коде |
|---|---|
| Генератор сообщений раз в секунду от IoT-устройств → Kafka | `generator/generator.py` |
| DDL/DML справочника типов IoT-устройств (id, TypeName) | `postgres/ddl.sql`, `postgres/dml.sql` |
| Источник (source) на SQL/Table API | `kafka_source_ddl()`, `jdbc_dim_ddl()` в `flink/job/flink_job.py` |
| Получатель (sink) на SQL/Table API | `kafka_sink_ddl()` в `flink/job/flink_job.py` |
| Переход между DataStream и SQL/Table API | `to_data_stream(...)` и `from_data_stream(...)` |
| Работа в event time | `WATERMARK FOR event_time ...` в DDL + `WatermarkStrategy` на DataStream |
| Соединение событий со статическим справочником из Postgres | `ENRICH_SQL` — `LEFT JOIN ... FOR SYSTEM_TIME AS OF` (lookup join) |
| Средняя температура за минуту | `AvgTempMedianHumidity` (event-time tumbling window 1 мин) |
| Медиана влажности за минуту | та же функция, `statistics.median(...)` |
| Формат результата (hh:mm, тип из pg, avg temp, median hum) → Kafka | `iot-aggregates`, JSON |

> **Почему медиана считается в Python, а не в SQL.** Во Flink встроенная функция
> `PERCENTILE` появилась только во Flink 2.0. На стабильном Flink 1.20 медианы в SQL нет,
> поэтому она вычисляется в `ProcessWindowFunction` после перехода в DataStream API —
> это заодно естественно демонстрирует переход Table → DataStream → Table.

## Версии (проверены под Flink 1.20)

| Компонент | Версия |
|---|---|
| Apache Flink / PyFlink | 1.20.1 |
| flink-sql-connector-kafka | 3.3.0-1.20 |
| flink-connector-jdbc (shaded, диалект Postgres) | 3.3.0-1.20 |
| PostgreSQL JDBC driver | 42.7.4 |
| Apache Kafka (KRaft) | 3.9.0 |
| PostgreSQL | 16 |
| confluent-kafka (генератор) | 2.6.1 |

## Структура проекта

```
flink-iot-final-project/
├── docker-compose.yml          # весь стек
├── Makefile                    # up / down / logs / events / aggregates / pg-check
├── flink/
│   ├── Dockerfile              # Flink 1.20 + PyFlink + коннекторы
│   └── job/flink_job.py        # PyFlink-джоба (sources/sink на Table API + переходы)
├── generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── generator.py            # генератор IoT-сообщений
└── postgres/
    ├── ddl.sql                 # CREATE TABLE device_types
    └── dml.sql                 # наполнение справочника
```