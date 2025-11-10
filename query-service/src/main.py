import asyncio
import json
import kuzu

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMsg

from .config import (
    NATS_HOST,
    NATS_PORT,
    NATS_DB_QUERY_TOPIC as topic,
    KUZU_DB_PATH,      
    get_logger,
)

from .modules.text2cypher import generate_cypher

logger = get_logger("main")


def run_query(question: str) -> dict:
    db = kuzu.Database(KUZU_DB_PATH, read_only=True)
    conn = kuzu.Connection(db)

    cypher, _ = generate_cypher(question, conn)
    res = conn.execute(cypher)

    return {
        "question": question,
        "cypher": cypher,
        "columns": list(res.column_names()),
        "rows": [list(r) for r in res],
    }


async def message_handler(msg: NATSMsg):
    data = msg.data.decode()
    logger.debug(f"Received a message on '{msg.subject}': {data}")

    try:
        payload = json.loads(data)
        question = payload.get("question")
        if not question:
            await msg.respond(b'{"error":"missing question"}')
            return

        result = run_query(question)
        await msg.respond(json.dumps(result).encode())

    except Exception as e:
        logger.error(f"Query failed: {e}")
        await msg.respond(json.dumps({"error": str(e)}).encode())


async def main():
    logger.info("Starting Query Service...")

    nc = NATS()
    await nc.connect(f"nats://{NATS_HOST}:{NATS_PORT}")
    await nc.subscribe(topic, cb=message_handler)

    logger.debug(f"Subscribed to '{topic}' on {NATS_HOST}:{NATS_PORT}")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
