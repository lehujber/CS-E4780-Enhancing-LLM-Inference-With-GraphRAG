import os
import logging
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=logging.INFO)
baseLogger = logging.getLogger("answer-service")
baseLogger.setLevel(LOG_LEVEL)

def get_logger(name: str) -> logging.Logger:
    return baseLogger.getChild(name)

NATS_HOST = os.getenv("NATS_HOST", "nats-server")
NATS_PORT = int(os.getenv("NATS_PORT", 4222))
NATS_ANSWER_TOPIC = os.getenv("NATS_ANSWER_TOPIC", "answer")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")