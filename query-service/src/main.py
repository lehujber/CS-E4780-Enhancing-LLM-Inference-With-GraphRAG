import asyncio
import signal
import json
import kuzu
import time

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMsg

from .config import (
    NATS_HOST,
    NATS_PORT,
    NATS_DB_QUERY_TOPIC as topic,
    KUZU_DB_PATH,      
    get_logger,
)

shutdown = False
def handle_shutdown(signum, frame):
    global shutdown
    shutdown = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

from .modules.text2cypher import generate_cypher, repair_cypher, get_schema_dict

logger = get_logger("main")


def self_refinement_loop(question: str, full_schema: dict, conn: kuzu.Connection) -> tuple[str, dict]:
    timings = {
        "initial_generation_time_ms": 0.0,
        "retries": [],
        "total_llm_time_ms": 0.0,
        "total_validation_time_ms": 0.0
    }
    
    # 1. Generate
    t0 = time.perf_counter()
    cypher, pruned_schema = generate_cypher(question, full_schema)
    gen_time = (time.perf_counter() - t0) * 1000
    timings["initial_generation_time_ms"] = gen_time
    timings["total_llm_time_ms"] += gen_time

    # Self-correction loop
    max_retries = 3
    for attempt in range(max_retries):
        retry_info = {"attempt": attempt + 1, "validation_time_ms": 0.0, "repair_time_ms": 0.0, "status": "unknown"}
        try:
            # 2. Validate (dry-run)
            # EXPLAIN checks syntax and binding without running the full query plan
            t0 = time.perf_counter()
            conn.execute(f"EXPLAIN {cypher}")
            val_time = (time.perf_counter() - t0) * 1000
            retry_info["validation_time_ms"] = val_time
            timings["total_validation_time_ms"] += val_time
            
            retry_info["status"] = "success"
            timings["retries"].append(retry_info)
            
            logger.info(f"Validation succeeded for query: {cypher}")
            break
        except Exception as e:
            val_time = (time.perf_counter() - t0) * 1000
            retry_info["validation_time_ms"] = val_time
            timings["total_validation_time_ms"] += val_time
            retry_info["status"] = "failed"
            
            logger.warning(f"Validation failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                logger.error("Max retries reached. Proceeding with potentially invalid query.")
                timings["retries"].append(retry_info)
                break
            
            # 3. Repair
            t0 = time.perf_counter()
            cypher = repair_cypher(question, cypher, str(e), pruned_schema, full_schema)
            rep_time = (time.perf_counter() - t0) * 1000
            retry_info["repair_time_ms"] = rep_time
            timings["total_llm_time_ms"] += rep_time
            
            timings["retries"].append(retry_info)
            logger.info(f"Repaired query: {cypher}")

    return cypher, timings


async def message_handler(msg: NATSMsg):
    data = msg.data.decode()
    logger.debug(f"Received a message on '{msg.subject}': {data}")

    try:
        payload = json.loads(data)
        question = payload.get("question")
        if not question:
            await msg.respond(b'{"error":"missing question"}')
            return

        db = kuzu.Database(KUZU_DB_PATH, read_only=True)
        conn = kuzu.Connection(db)

        full_schema = get_schema_dict(conn)
        # logger.debug(f"Full schema: {full_schema}")

        cypher, timings = self_refinement_loop(question, full_schema, conn)

        t0 = time.perf_counter()
        res = conn.execute(cypher)
        timings["db_execution_time_ms"] = (time.perf_counter() - t0) * 1000

        result = {
            "question": question,
            "cypher": cypher,
            "columns": res.get_column_names(), # In Kuzu 0.11+, use get_column_names() instead of column_names()
            "rows": [list(r) for r in res],
            "timings": timings
        }

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

    while not shutdown:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
