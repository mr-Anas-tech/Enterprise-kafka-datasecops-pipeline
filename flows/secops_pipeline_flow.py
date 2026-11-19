import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from prefect.cache_policies import NO_CACHE
from confluent_kafka import Consumer, KafkaError
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
import snowflake.connector


def get_secret_value(block_name: str, env_fallback: str) -> str:
    """Load secret value from Prefect block or environment variable."""
    try:
        return Secret.load(block_name).get()
    except Exception:
        return os.environ[env_fallback]


def get_kafka_config() -> dict[str, Any]:
    """Get Kafka config from Prefect block or environment variable."""
    return {
        "bootstrap.servers": get_secret_value("kafka-bootstrap-servers", "KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": get_secret_value("kafka-api-key", "KAFKA_API_KEY"),
        "sasl.password": get_secret_value("kafka-api-secret", "KAFKA_API_SECRET"),
        "group.id": "secops-snowflake-loader-prod",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


@task(name="consume_kafka_secops", retries=2, retry_delay_seconds=5)
def consume_kafka_batch(
    topic: str = "secops_networks_logs",
    max_messages: int = 5000,
    poll_timeout: float = 2.0,
) -> tuple[list[dict[str, Any]], Consumer]:
    """Pull a micro-batch of SecOps JSON events from Confluent Cloud Kafka."""
    logger = get_run_logger()
    consumer = Consumer(get_kafka_config())
    consumer.subscribe([topic])

    records: list[dict[str, Any]] = []
    empty_polls = 0

    try:
        while len(records) < max_messages and empty_polls < 3:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {msg.error()}")

            empty_polls = 0

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                payload["_kafka_partition"] = msg.partition()
                payload["_kafka_offset"] = msg.offset()
                payload["_kafka_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
                records.append(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as err:
                logger.warning(f"Skipping malformed message at offset {msg.offset()}: {err}")

        logger.info(f"Consumed {len(records)} valid SecOps records from '{topic}'.")
        return records, consumer
    except Exception:
        consumer.close()
        raise


def get_snowflake_config() -> dict[str, str]:
    """Build Snowflake connection configuration from Prefect Secrets or env vars."""
    return {
        "account": get_secret_value("snowflake-account", "SNOWFLAKE_ACCOUNT"),
        "user": get_secret_value("snowflake-user", "SNOWFLAKE_USER"),
        "password": get_secret_value("snowflake-password", "SNOWFLAKE_PASSWORD"),
        "warehouse": get_secret_value("snowflake-warehouse", "SNOWFLAKE_WAREHOUSE"),
        "database": get_secret_value("snowflake-database", "SNOWFLAKE_DATABASE"),
        "schema": get_secret_value("snowflake-schema", "SNOWFLAKE_SCHEMA"),
    }


@task(name="load-secops-logs-to-snowflake", cache_policy=NO_CACHE)
def load_to_snowflake(
    records: list[dict[str, Any]],
    consumer: Consumer,
    table_name: str = "SECOPS_NETWORK_LOGS_RAW",
) -> int:
    """Load consumed SecOps JSON records into Snowflake RAW table and commit Kafka offsets."""
    logger = get_run_logger()

    if not records:
        logger.info("No new Kafka records to load into Snowflake. Closing consumer.")
        consumer.close()
        return 0

    sf_cfg = get_snowflake_config()
    conn = None
    cursor = None

    try:
        conn = snowflake.connector.connect(
            account=sf_cfg["account"],
            user=sf_cfg["user"],
            password=sf_cfg["password"],
            warehouse=sf_cfg["warehouse"],
            database=sf_cfg["database"],
            schema=sf_cfg["schema"],
        )
        cursor = conn.cursor()

        full_table = f"{sf_cfg['database']}.{sf_cfg['schema']}.{table_name}"
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                RAW_PAYLOAD VARIANT,
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )

        insert_sql = f"""
            INSERT INTO {full_table} (RAW_PAYLOAD)
            VALUES (PARSE_JSON(%s))
        """
        rows = [(json.dumps(record),) for record in records]
        cursor.executemany(insert_sql, rows)
        conn.commit()

        consumer.commit(asynchronous=False)
        logger.info(f"Loaded {len(records)} records into {full_table} and committed Kafka offsets.")
        return len(records)

    except Exception as err:
        if conn:
            conn.rollback()
        logger.error(f"Failed loading records to Snowflake: {err}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        consumer.close()


@flow(name="datasecops-kafka-to-snowflake-pipeline", log_prints=True)
def secops_pipeline_flow(
    topic: str = "secops_networks_logs",
    max_messages: int = 5000,
    table_name: str = "SECOPS_NETWORK_LOGS_RAW",
) -> int:
    """Orchestrate the SecOps Kafka to Snowflake ETL pipeline."""
    logger = get_run_logger()
    logger.info(f"Starting SecOps Kafka to Snowflake pipeline for topic '{topic}' -> target_table '{table_name}'.")

    records, consumer = consume_kafka_batch(topic=topic, max_messages=max_messages)
    loaded_count = load_to_snowflake(records=records, consumer=consumer, table_name=table_name)

    logger.info(f"Pipeline completed. Total records loaded into Snowflake: {loaded_count}.")
    return loaded_count


if __name__ == "__main__":
    secops_pipeline_flow()
