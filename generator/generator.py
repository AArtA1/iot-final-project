#!/usr/bin/env python3
"""
Генератор сообщений от IoT-устройств.

Раз в секунду публикует в Kafka-топик событие вида:
    {
        "type_id":     <int>,      # тип устройства (id из справочника device_types)
        "event_time":  <str>,      # время события "YYYY-MM-DD HH:MM:SS.mmm" (UTC)
        "temperature": <float>,    # температура
        "humidity":    <float>     # влажность
    }

Время пишется в формате SQL-timestamp, который Flink JSON-формат читает при
'json.timestamp-format.standard' = 'SQL'.
"""

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Конфигурация (через переменные окружения, со значениями по умолчанию)
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot-events")
INTERVAL = float(os.getenv("GEN_INTERVAL_SECONDS", "1.0"))

# Типы устройств. id ОБЯЗАН совпадать со справочником в postgres/dml.sql.
# Для каждого типа задаём (mu, sigma) нормального распределения температуры и влажности,
# чтобы данные выглядели реалистично и по типам различались.
DEVICE_TYPES = {
    1: {"name": "Thermostat",          "temp": (20.0, 2.0), "hum": (45.0, 5.0)},
    2: {"name": "Humidity Sensor",     "temp": (22.0, 1.0), "hum": (60.0, 8.0)},
    3: {"name": "HVAC Controller",     "temp": (19.0, 3.0), "hum": (50.0, 6.0)},
    4: {"name": "Smart Plug",          "temp": (30.0, 4.0), "hum": (35.0, 4.0)},
    5: {"name": "Air Quality Monitor", "temp": (24.0, 1.5), "hum": (55.0, 7.0)},
}

_running = True


def _stop(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "iot-generator",
            "linger.ms": 50,
            "acks": "all",
        }
    )


def wait_for_broker(producer: Producer, attempts: int = 60) -> None:
    """Ждём, пока брокер станет доступен (контейнер kafka может стартовать дольше)."""
    for i in range(attempts):
        if not _running:
            sys.exit(0)
        try:
            producer.list_topics(timeout=5)
            print(f"[generator] Kafka доступна по адресу {BOOTSTRAP}", flush=True)
            return
        except Exception as exc:  # noqa: BLE001
            print(f"[generator] жду Kafka ({i + 1}/{attempts}): {exc}", flush=True)
            time.sleep(3)
    raise RuntimeError(f"Kafka недоступна по адресу {BOOTSTRAP}")


def make_event() -> dict:
    type_id = random.choice(list(DEVICE_TYPES.keys()))
    cfg = DEVICE_TYPES[type_id]
    t_mu, t_sd = cfg["temp"]
    h_mu, h_sd = cfg["hum"]

    now = datetime.now(timezone.utc)
    event_time = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}"

    return {
        "type_id": type_id,
        "event_time": event_time,
        "temperature": round(random.gauss(t_mu, t_sd), 2),
        "humidity": round(min(100.0, max(0.0, random.gauss(h_mu, h_sd))), 2),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[generator] ошибка доставки: {err}", flush=True)


def main() -> None:
    producer = build_producer()
    wait_for_broker(producer)

    print(
        f"[generator] публикую в топик '{TOPIC}' раз в {INTERVAL:.1f}с. Ctrl+C для остановки.",
        flush=True,
    )

    sent = 0
    while _running:
        event = make_event()
        producer.produce(
            TOPIC,
            key=str(event["type_id"]),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # обслуживаем колбэки доставки

        sent += 1
        if sent % 10 == 0:
            print(f"[generator] отправлено {sent}, последнее: {event}", flush=True)

        time.sleep(INTERVAL)

    print("[generator] останавливаюсь, дослав буфер...", flush=True)
    producer.flush(10)


if __name__ == "__main__":
    main()
