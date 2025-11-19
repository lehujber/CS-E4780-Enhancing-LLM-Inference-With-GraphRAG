from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import hashlib

from pymemcache.client import base

from nats.aio.client import Client as NATSClient

from .config import (
    MEMCACHE_HOST,
    MEMCACHE_PORT,
    NATS_HOST,
    NATS_PORT,
    NATS_DB_QUERY_TOPIC as db_query_topic,
    NATS_ANSWER_TOPIC as answer_topic,
    get_logger,
)

mem_client = base.Client((MEMCACHE_HOST, MEMCACHE_PORT))
nats_client = NATSClient()
@asynccontextmanager
async def lifespan(app: FastAPI):
    await nats_client.connect(servers=[f"nats://{NATS_HOST}:{NATS_PORT}"])

    logger.debug(f"Connected to NATS at nats://{NATS_HOST}:{NATS_PORT}")
    nats_conn = nats_client.connected_url

    if not nats_conn:
        logger.error("Failed to connect to NATS server")
    else:
        logger.debug(f"Successfully connected to NATS server on {nats_conn.geturl()} on topics {db_query_topic}, {answer_topic}")

    yield
    await nats_client.drain()
    mem_client.close()

logger = get_logger("main")

class QuestionRequest(BaseModel):
    question: str

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "Hello World"}

@app.get("/health")
async def health_check():
    logger.debug("Health check endpoint called")
    return {"status": "healthy"}

@app.post("/question")
async def answer_question(question: QuestionRequest):
    logger.debug(f"Question received: {question.question}")

    # Use a safe memcache key (hash of the question) because memcached keys
    # cannot contain whitespace or certain characters. We store and lookup by
    # the SHA-256 hex digest of the question string.
    cache_key = hashlib.sha256(question.question.encode()).hexdigest()
    cached_value = mem_client.get(cache_key)
    if cached_value:
        logger.debug(f"Cache hit for question: {question.question}")
        # pymemcache returns bytes for stored values; decode to string if needed
        if isinstance(cached_value, (bytes, bytearray)):
            cached_value = cached_value.decode()
        return {"question": question.question, "answer": cached_value}

    logger.debug(f"Cache miss for question: {question.question}")

    #TODO: implement calls to NATS and other services to get the answer

    try:
        logger.debug(f"Requested question on NATS topic {db_query_topic}")
        db_request_payload = json.dumps({"question": question.question}).encode()
        db_answer = await nats_client.request(db_query_topic, db_request_payload, timeout=30)
    
        data = db_answer.data.decode()
        preview = data[:200] + "..." if len(data) > 200 else data
        logger.debug(f"Received DB answer: {preview}")

        logger.debug(f"Requested answer on NATS topic {answer_topic}")
        question_answer = await nats_client.request(answer_topic, data.encode(), timeout=30)
    
        answer = question_answer.data.decode()

        # Store the answer under the hashed key to avoid illegal key errors
        mem_client.set(cache_key, answer)

        return {"question": question.question, "answer": answer}
    except Exception as e:
        logger.error(f"Failed to retrieve data from db service: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
