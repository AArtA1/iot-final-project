# Удобные команды. Требуется Docker + Docker Compose v2.

.PHONY: up down logs ps clean resubmit events aggregates pg-check

## Поднять весь стек (с пересборкой образов)
up:
	docker compose up --build -d
	@echo ""
	@echo "Flink UI : http://localhost:8081"
	@echo "Kafka UI : http://localhost:8080"
	@echo "Логи     : make logs"

## Остановить и удалить контейнеры
down:
	docker compose down

## Полная очистка (контейнеры + тома + образы проекта)
clean:
	docker compose down -v --rmi local

## Логи всех сервисов
logs:
	docker compose logs -f --tail=100

## Статус сервисов
ps:
	docker compose ps

## Пересабмитить джобу (например, после правки flink_job.py — сначала пересобери образ)
resubmit:
	docker compose up -d --build jobmanager taskmanager
	docker compose run --rm job-submitter

## Смотреть входной поток событий
events:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 --topic iot-events --from-beginning

## Смотреть результат (агрегаты по минутам)
aggregates:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 --topic iot-aggregates --from-beginning

## Проверить справочник в Postgres
pg-check:
	docker compose exec postgres psql -U iot -d iot -c "SELECT * FROM device_types ORDER BY id;"
