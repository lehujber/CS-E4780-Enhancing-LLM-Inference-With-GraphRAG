import asyncio
import os
import datetime
import json
import requests as reqs

from nats.aio.client import Client as NATS

NATS_HOST = os.getenv("NATS_HOST", "nats-server")
NATS_PORT = int(os.getenv("NATS_PORT", 4222))
NATS_DB_QUERY_TOPIC = os.getenv("NATS_DB_QUERY_TOPIC", "db-query")
NATS_ANSWER_TOPIC = os.getenv("NATS_ANSWER_TOPIC", "answer")

API_ENDPOINT_URL = os.getenv("API_ENDPOINT_URL", "http://question-api:8000/question")


questions = []

async def main():
    await asyncio.sleep(10)

    benchmarks_internal = [benchmark_internal(q) for q in questions]
    results_internal = await asyncio.gather(*benchmarks_internal)

    benchmarks_end_to_end = [benchmark_end_to_end(q) for q in questions]
    results_end_to_end = await asyncio.gather(*benchmarks_end_to_end)

    total_query_gen_delay = sum(res["timings"]["db_query_time_ms"] for res in results_internal)
    total_answer_gen_delay = sum(res["timings"]["answer_time_ms"] for res in results_internal)


    with open("/benchmark-data/benchmark_internal_results.json", "w") as f:
        output = {
            "results": results_internal,
            "total_query_gen_delay_ms": total_query_gen_delay,
            "total_answer_gen_delay_ms": total_answer_gen_delay,
        }
        json.dump(output, f, indent=4)

    total_request_delay = sum(res["timings"]["total_request_time_ms"] for res in results_end_to_end)
    with open("/benchmark-data/benchmark_end_to_end_results.json", "w") as f:
        output = {
            "results": results_end_to_end,
            "total_request_delay_ms": total_request_delay,
        }
        json.dump(output, f, indent=4)

async def benchmark_internal(question: str) -> dict:
    nc = NATS()
    await nc.connect(f"nats://{NATS_HOST}:{NATS_PORT}")

    query_start = datetime.datetime.now()
    msg = await nc.request(NATS_DB_QUERY_TOPIC, question.encode())
    query_end = datetime.datetime.now()

    db_query = msg.data.decode()

    answer_start = datetime.datetime.now()
    msg = await nc.request(NATS_ANSWER_TOPIC, db_query.encode())
    answer_end = datetime.datetime.now()

    answer = msg.data.decode()

    return {
        "question": question,
        "answer": answer,
        "timings": {
            "db_query_time_ms": (query_end - query_start).total_seconds() * 1000,
            "answer_time_ms": (answer_end - answer_start).total_seconds() * 1000,
        }
    }

async def benchmark_end_to_end(question: str) -> dict:
    request_start = datetime.datetime.now()
    response = reqs.post(API_ENDPOINT_URL, json={"question": question})
    request_end = datetime.datetime.now()

    return {
        "question": question,
        "answer": response.json().get("answer", ""),
        "timings": {
            "total_request_time_ms": (request_end - request_start).total_seconds() * 1000,
        }
    }

if __name__ == "__main__":
    asyncio.run(main())