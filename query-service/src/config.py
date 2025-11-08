import os
import logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=logging.INFO)
baseLogger = logging.getLogger("query-service")
baseLogger.setLevel(LOG_LEVEL)

def get_logger(name: str) -> logging.Logger:
    return baseLogger.getChild(name)

NATS_HOST = os.getenv("NATS_HOST", "nats-server")
NATS_PORT = int(os.getenv("NATS_PORT", 4222))
NATS_DB_QUERY_TOPIC = os.getenv("NATS_DB_QUERY_TOPIC", "db-query")