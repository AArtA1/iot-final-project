#!/usr/bin/env python3
"""
Заключительный проект Apache Flink (PyFlink, Flink 1.20).

Конвейер (полностью повторяет схему из задания):

    kafka[iot-events]  ──(source на Table API / SQL DDL)──┐
                                                          ├─► lookup-join ─► to_data_stream
    postgres[device_types] ─(jdbc source на Table API)────┘                      │
                                                                                 ▼
                                                       event-time tumbling window (1 минута):
                                                         AVG(temperature), MEDIAN(humidity)
                                                                                 │
                                          kafka[iot-aggregates] ◄─(sink на Table API)◄─ from_data_stream

Что из требований задания где реализовано:
  * Источник (source) на sql/table api .......... CREATE TABLE iot_events (kafka), device_types (jdbc)
  * Получатель (sink) на sql/table api ........... CREATE TABLE iot_aggregates (kafka)
  * Переход datastream <-> sql/table api ......... to_data_stream(...) и from_data_stream(...)
  * Работа в event time .......................... WATERMARK в DDL + WatermarkStrategy на DataStream
  * Соединение со статическим справочником ....... LEFT JOIN ... FOR SYSTEM_TIME AS OF (lookup join)
  * Средняя температура и медиана влажности ...... ProcessWindowFunction в DataStream API

Все времена — UTC (генератор пишет event_time в UTC, окно тоже считается в UTC).
"""

import statistics
from datetime import datetime, timezone

from pyflink.common import Duration, Row
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

# ---------------------------------------------------------------------------
# Параметры подключения (совпадают с docker-compose.yml)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC = "iot-events"
SINK_TOPIC = "iot-aggregates"

PG_URL = "jdbc:postgresql://postgres:5432/iot"
PG_TABLE = "device_types"
PG_USER = "iot"
PG_PASSWORD = "iot"

WINDOW_MINUTES = 1
MAX_OUT_OF_ORDERNESS_SECONDS = 5


# ---------------------------------------------------------------------------
# DDL источников и приёмника — это и есть "source/sink на Table API"
# ---------------------------------------------------------------------------
def kafka_source_ddl() -> str:
    return f"""
    CREATE TABLE iot_events (
        type_id      INT,
        event_time   TIMESTAMP(3),
        temperature  DOUBLE,
        humidity     DOUBLE,
        proc_time    AS PROCTIME(),                                   -- для lookup-join
        WATERMARK FOR event_time AS event_time
                  - INTERVAL '{MAX_OUT_OF_ORDERNESS_SECONDS}' SECOND  -- event time
    ) WITH (
        'connector'                       = 'kafka',
        'topic'                           = '{SOURCE_TOPIC}',
        'properties.bootstrap.servers'    = '{KAFKA_BOOTSTRAP}',
        'properties.group.id'             = 'flink-iot-job',
        'scan.startup.mode'               = 'earliest-offset',
        'format'                          = 'json',
        'json.timestamp-format.standard'  = 'SQL',
        'json.fail-on-missing-field'      = 'false',
        'json.ignore-parse-errors'        = 'true'
    )
    """


def jdbc_dim_ddl() -> str:
    # Статический справочник из Postgres, читаем как lookup-источник (с кэшем).
    return f"""
    CREATE TABLE device_types (
        id        INT,
        type_name STRING
    ) WITH (
        'connector'                                = 'jdbc',
        'url'                                      = '{PG_URL}',
        'table-name'                               = '{PG_TABLE}',
        'username'                                 = '{PG_USER}',
        'password'                                 = '{PG_PASSWORD}',
        'lookup.cache'                             = 'PARTIAL',
        'lookup.partial-cache.max-rows'            = '1000',
        'lookup.partial-cache.expire-after-write'  = '10 min'
    )
    """


def kafka_sink_ddl() -> str:
    return f"""
    CREATE TABLE iot_aggregates (
        window_time      STRING,   -- Время (hh:mm)
        type_name        STRING,   -- Тип устройства (из pg)
        avg_temperature  DOUBLE,   -- Средняя температура
        median_humidity  DOUBLE    -- Медиана влажности
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = '{SINK_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'format'                       = 'json'
    )
    """


# Запрос обогащения: соединяем поток из Kafka со статическим справочником из Postgres.
# FOR SYSTEM_TIME AS OF e.proc_time — это lookup-join (temporal join по processing time).
ENRICH_SQL = """
    SELECT
        e.event_time   AS event_time,
        e.type_id      AS type_id,
        d.type_name    AS type_name,
        e.temperature  AS temperature,
        e.humidity     AS humidity
    FROM iot_events AS e
    LEFT JOIN device_types FOR SYSTEM_TIME AS OF e.proc_time AS d
        ON e.type_id = d.id
"""


# ---------------------------------------------------------------------------
# Извлечение event-time из строки DataStream (поле event_time — naive UTC datetime)
# ---------------------------------------------------------------------------
class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        dt = value[0]  # event_time
        # TIMESTAMP(3) приходит как naive datetime; трактуем его как UTC.
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# Оконная функция: средняя температура + медиана влажности за окно
# ---------------------------------------------------------------------------
class AvgTempMedianHumidity(ProcessWindowFunction):
    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        temperatures = []
        humidities = []
        for row in elements:
            temperatures.append(row[2])  # temperature
            humidities.append(row[3])    # humidity

        if not temperatures:
            return

        avg_temp = sum(temperatures) / len(temperatures)
        median_hum = statistics.median(humidities)  # в SQL медианы нет — считаем здесь

        # context.window().start — начало окна (epoch millis, UTC) -> "hh:mm"
        start_ms = context.window().start
        hhmm = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%H:%M")

        yield Row(hhmm, key, round(avg_temp, 2), round(median_hum, 2))


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    # 1. Регистрируем источники и приёмник через Table API (SQL DDL).
    t_env.execute_sql(kafka_source_ddl())
    t_env.execute_sql(jdbc_dim_ddl())
    t_env.execute_sql(kafka_sink_ddl())

    # 2. Обогащаем поток справочником (join в SQL / Table API).
    enriched = t_env.sql_query(ENRICH_SQL)

    # 3. Переход Table -> DataStream.
    ds = t_env.to_data_stream(enriched)

    # 4. Назначаем watermark'и на DataStream и считаем оконную агрегацию в event time.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(MAX_OUT_OF_ORDERNESS_SECONDS))
        .with_timestamp_assigner(EventTimeAssigner())
    )

    # Table -> DataStream carries event_time as LOCAL_DATE_TIME, for which PyFlink has no
    # window/list-state coder (ValueError: Unsupported type_info LocalTimeTypeInfo). Once the
    # watermarks are assigned we no longer need that column (the window uses context.window()),
    # so project the row down to fully-serializable fields before the keyed window.
    stamped_ds = ds.assign_timestamps_and_watermarks(watermark_strategy).map(
        lambda row: Row(row[1], row[2], row[3], row[4]),
        output_type=Types.ROW_NAMED(
            ["type_id", "type_name", "temperature", "humidity"],
            [Types.INT(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
        ),
    )

    result_ds = (
        stamped_ds
          .key_by(lambda row: row[1] if row[1] is not None else "UNKNOWN",
                  key_type=Types.STRING())
          .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
          .process(
              AvgTempMedianHumidity(),
              output_type=Types.ROW_NAMED(
                  ["window_time", "type_name", "avg_temperature", "median_humidity"],
                  [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
              ),
          )
    )

    # 5. Переход DataStream -> Table и запись в Kafka-приёмник (sink на Table API).
    result_table = t_env.from_data_stream(result_ds)
    result_table.execute_insert("iot_aggregates")


if __name__ == "__main__":
    main()
