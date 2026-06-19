FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# -u: небуферизованный вывод, чтобы логи сразу были видны в `docker compose logs`
CMD ["python", "-u", "generator.py"]
