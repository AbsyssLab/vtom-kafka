#!/usr/bin/env python3
"""
Kafka Job script for VTOM: wait for a message then succeed/fail (exit 0/1).

Positional parameters:
  $1  bootstrap_servers
  $2  topic
  $3  group_id
  $4  expected_text
  $5  commit_mode         optional (default: 0)
                          0 = no commit
                          1 = commit fetched batch
                          2 = commit up to first matching message
  $6  wait_timeout_sec    optional (default: 0)
                          how long to stay open waiting for the message (seconds)
                          if the message does not arrive in time -> exit code 1

Environment variables:
  KAFKA_EXPECTED_IDS        comma-separated IDs (optional)
  KAFKA_SECURITY_PROTOCOL   default: PLAINTEXT
  KAFKA_SASL_MECHANISM
  KAFKA_USERNAME
  KAFKA_PASSWORD
  KAFKA_SSL_CAFILE

Exit codes:
  0 = matching message found
  1 = timeout / no matching message / error
"""

import logging
import os
import sys
import time

from kafka import KafkaConsumer
from kafka.serializer import DefaultSerializer
from kafka.structs import OffsetAndMetadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
    force=True,
)

# Silence verbose kafka-python internal logs
logging.getLogger("kafka").setLevel(logging.WARNING)

# === Configuration from environment ===
KAFKA_EXPECTED_IDS = os.getenv("KAFKA_EXPECTED_IDS", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE", "")

COMMIT_MODE_LABELS = {
    0: "none",
    1: "full batch",
    2: "up to first match",
}


def parse_commit_mode(value):
    """Parse $5 commit mode: 0=none, 1=batch, 2=first match."""
    raw = str(value).strip().lower()
    if raw in {"", "0", "false", "no", "n"}:
        return 0
    if raw in {"1", "true", "yes", "y", "batch"}:
        return 1
    if raw in {"2", "first"}:
        return 2
    raise ValueError(
        f"Invalid $5 value '{value}'. Expected: 0 (none), 1 (batch), 2 (first match)"
    )


def parse_wait_timeout(value):
    """Parse $6 wait timeout in seconds."""
    raw = str(value).strip()
    if raw == "":
        return 0.0
    try:
        timeout = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid $6 value '{value}'. Expected duration in seconds (e.g. 0, 30, 120)"
        ) from exc
    if timeout < 0:
        raise ValueError("Invalid $6 value: wait timeout cannot be negative")
    return timeout


# === Argument validation ===
if len(sys.argv) < 5:
    logging.error(
        "Usage: python kafkaJob.py <bootstrap_servers> <topic> <group_id> "
        "<expected_text> [commit_mode] [wait_timeout_sec]"
    )
    logging.error(
        "Environment variables: KAFKA_EXPECTED_IDS, KAFKA_SECURITY_PROTOCOL, "
        "KAFKA_SASL_MECHANISM, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_SSL_CAFILE"
    )
    sys.exit(1)

BOOTSTRAP_SERVERS = sys.argv[1]
TOPIC = sys.argv[2]
GROUP_ID = sys.argv[3]
EXPECTED_TEXT = sys.argv[4]

try:
    COMMIT_MODE = parse_commit_mode(sys.argv[5] if len(sys.argv) > 5 else "0")
    WAIT_TIMEOUT_SEC = parse_wait_timeout(sys.argv[6] if len(sys.argv) > 6 else "0")
except ValueError as exc:
    logging.error(str(exc))
    sys.exit(1)

EXPECTED_IDS = [item.strip() for item in KAFKA_EXPECTED_IDS.split(",") if item.strip()]


def build_consumer():
    """Build a configured KafkaConsumer."""
    config = {
        "bootstrap_servers": [
            item.strip() for item in BOOTSTRAP_SERVERS.split(",") if item.strip()
        ],
        "group_id": GROUP_ID,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "request_timeout_ms": 10000,
        "session_timeout_ms": 10000,
        "security_protocol": KAFKA_SECURITY_PROTOCOL or "PLAINTEXT",
        "value_deserializer": DefaultSerializer(),
    }

    if KAFKA_SASL_MECHANISM:
        config["sasl_mechanism"] = KAFKA_SASL_MECHANISM
    if KAFKA_USERNAME:
        config["sasl_plain_username"] = KAFKA_USERNAME
    if KAFKA_PASSWORD:
        config["sasl_plain_password"] = KAFKA_PASSWORD
    if KAFKA_SSL_CAFILE:
        config["ssl_cafile"] = KAFKA_SSL_CAFILE

    return KafkaConsumer(TOPIC, **config)


def wait_for_assignment(consumer, timeout_sec=10):
    """Wait until Kafka assigns partitions to the consumer."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        # Required to trigger rebalance; may consume messages that we rewind later
        consumer.poll(timeout_ms=200)
        if consumer.assignment():
            return True
    return False


def reset_positions(consumer):
    """
    Seek to the group's committed offset, or to the beginning if none exists.
    Prevents losing messages consumed during assignment wait.
    """
    for tp in sorted(consumer.assignment(), key=lambda item: item.partition):
        committed = consumer.committed(tp)
        beginning = consumer.beginning_offsets([tp])[tp]
        end = consumer.end_offsets([tp])[tp]

        if committed is None:
            consumer.seek_to_beginning(tp)
            position = consumer.position(tp)
            logging.info(
                f"Partition {tp.partition}: no commit -> begin "
                f"(begin={beginning} end={end} position={position})"
            )
        else:
            consumer.seek(tp, committed)
            position = consumer.position(tp)
            logging.info(
                f"Partition {tp.partition}: commit={committed} "
                f"end={end} remaining={max(0, end - position)}"
            )


def message_is_valid(payload):
    """Return True when payload matches expected text/IDs."""
    if EXPECTED_TEXT not in payload:
        return False
    if not EXPECTED_IDS:
        return True
    return any(expected_id in payload for expected_id in EXPECTED_IDS)


def do_commit(consumer, topic_partition, message):
    """Apply commit according to mode 0 / 1 / 2."""
    if COMMIT_MODE == 0:
        logging.info("Commit not requested")
        return

    if COMMIT_MODE == 1:
        consumer.commit()
        logging.info("Commit done (full batch)")
        return

    next_offset = message.offset + 1
    consumer.commit({
        topic_partition: OffsetAndMetadata(next_offset, "")
    })
    logging.info(
        f"Commit done up to first match "
        f"(partition={topic_partition.partition}, next_offset={next_offset})"
    )


def scan_records(records, consumer):
    """Scan one poll result and return True if a matching message is found."""
    if not records:
        return False

    for topic_partition, messages in records.items():
        for message in messages:
            payload = "" if message.value is None else str(message.value)
            if message_is_valid(payload):
                logging.info(
                    f"Matching message found "
                    f"(offset={message.offset}, partition={topic_partition.partition})"
                )
                do_commit(consumer, topic_partition, message)
                return True

    return False


# === Main execution ===
consumer = None
try:
    logging.info(
        f"Starting Kafka job | servers={BOOTSTRAP_SERVERS} topic={TOPIC} "
        f"group={GROUP_ID} text={EXPECTED_TEXT} "
        f"ids={EXPECTED_IDS if EXPECTED_IDS else 'none'} "
        f"commit={COMMIT_MODE}({COMMIT_MODE_LABELS[COMMIT_MODE]}) "
        f"wait={WAIT_TIMEOUT_SEC}s protocol={KAFKA_SECURITY_PROTOCOL}"
    )

    consumer = build_consumer()
    logging.info("Connected to Kafka")

    if not wait_for_assignment(consumer):
        logging.error("No partition assigned (timeout)")
        sys.exit(1)

    assigned = sorted(consumer.assignment(), key=lambda tp: tp.partition)
    logging.info(
        "Assigned partitions: "
        + ", ".join(f"{tp.topic}:{tp.partition}" for tp in assigned)
    )

    reset_positions(consumer)
    logging.info(f"Listening (max wait={WAIT_TIMEOUT_SEC}s)...")

    deadline = time.time() + WAIT_TIMEOUT_SEC
    while True:
        remaining_ms = max(0, int((deadline - time.time()) * 1000))
        # Short poll even when wait=0 so we still get a chance to read
        poll_ms = 1000 if WAIT_TIMEOUT_SEC > 0 else 500
        if WAIT_TIMEOUT_SEC > 0:
            poll_ms = min(poll_ms, max(remaining_ms, 1))

        records = consumer.poll(timeout_ms=poll_ms, max_records=50)
        if scan_records(records, consumer):
            logging.info("Result: OK (code 0)")
            sys.exit(0)

        if WAIT_TIMEOUT_SEC <= 0 or time.time() >= deadline:
            break

    if WAIT_TIMEOUT_SEC > 0:
        logging.error(f"Timeout: no matching message after {WAIT_TIMEOUT_SEC}s")
    else:
        logging.info("No matching message available")
    logging.info("Result: KO (code 1)")
    sys.exit(1)

except Exception as exc:
    logging.error(f"Error during execution: {exc}")
    sys.exit(1)

finally:
    if consumer is not None:
        logging.info("Closing Kafka consumer")
        consumer.close()
