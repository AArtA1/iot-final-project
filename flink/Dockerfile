# ============================================================================
#  Образ Flink 1.20 + Python/PyFlink + коннекторы (Kafka, JDBC, Postgres-драйвер).
#  Используется одновременно как JobManager, TaskManager и клиент для сабмита.
# ============================================================================
FROM flink:1.20.1

# --- Python + PyFlink ---------------------------------------------------------
# Ставим PyFlink в изолированный venv: так не упираемся в PEP 668
# (externally-managed-environment) и не ломаем системный python.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        python3 python3-venv python3-pip python3-dev build-essential curl ca-certificates \
        openjdk-11-jdk-headless; \
    rm -rf /var/lib/apt/lists/*; \
    # On arm64 there is no prebuilt pemja/apache-flink wheel, so they compile from source.
    # That needs a C toolchain (build-essential), CPython headers (python3-dev), and JNI
    # headers at $JAVA_HOME/include. The base flink image ships only a JRE, so we expose
    # the JDK headers we just installed. (All a no-op on amd64, which uses prebuilt wheels.)
    if [ ! -d /opt/java/openjdk/include ]; then \
        ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")/include" /opt/java/openjdk/include; \
    fi; \
    python3 -m venv /opt/pyflink-venv; \
    /opt/pyflink-venv/bin/pip install --no-cache-dir --upgrade pip; \
    /opt/pyflink-venv/bin/pip install --no-cache-dir apache-flink==1.20.1

# Делаем venv-python основным, чтобы TaskManager и клиент `flink run -py`
# исполняли python-код именно с установленным PyFlink.
ENV PATH="/opt/pyflink-venv/bin:${PATH}"
RUN ln -sf /opt/pyflink-venv/bin/python  /usr/local/bin/python && \
    ln -sf /opt/pyflink-venv/bin/python  /usr/local/bin/python3

# --- Коннекторы в /opt/flink/lib (подхватываются JM и TM автоматически) --------
ARG FLINK_KAFKA_VER=3.3.0-1.20
ARG FLINK_JDBC_VER=3.3.0-1.20
ARG PG_DRIVER_VER=42.7.4
ARG MVN=https://repo1.maven.org/maven2

RUN set -eux; cd /opt/flink/lib; \
    curl -fSL -o "flink-sql-connector-kafka-${FLINK_KAFKA_VER}.jar" \
        "${MVN}/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VER}/flink-sql-connector-kafka-${FLINK_KAFKA_VER}.jar"; \
    curl -fSL -o "flink-connector-jdbc-${FLINK_JDBC_VER}.jar" \
        "${MVN}/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_VER}/flink-connector-jdbc-${FLINK_JDBC_VER}.jar"; \
    curl -fSL -o "postgresql-${PG_DRIVER_VER}.jar" \
        "${MVN}/org/postgresql/postgresql/${PG_DRIVER_VER}/postgresql-${PG_DRIVER_VER}.jar"

# --- Код джобы ----------------------------------------------------------------
COPY job /opt/flink/usrlib/job
