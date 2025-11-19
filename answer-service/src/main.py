import asyncio
import signal
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMsg

from .config import (
    NATS_HOST,
    NATS_PORT,
    NATS_ANSWER_TOPIC as topic,
    get_logger
)

shutdown = False
def handle_shutdown(signum, frame):
    global shutdown
    shutdown = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

logger = get_logger("main")

async def message_handler(msg: NATSMsg):
    data = msg.data.decode()
    logger.debug(f"Received a message on '{msg.subject}': {data}")

    # TODO: implement answer generation
    await msg.respond(b"Sample answer response")

async def main():
    logger.info("Starting Answer Service...")


    nc = NATS()
    await nc.connect(f"nats://{NATS_HOST}:{NATS_PORT}")
    await nc.subscribe(topic, cb=message_handler)

    logger.debug(f"Subscribed to topic '{topic}' on NATS server at {NATS_HOST}:{NATS_PORT}")

    while not shutdown:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())