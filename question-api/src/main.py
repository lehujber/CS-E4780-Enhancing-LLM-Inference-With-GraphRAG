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
        # pymemcache returns bytes for stored values; decode as UTF-8
        if isinstance(cached_value, (bytes, bytearray)):
            cached_value = cached_value.decode('utf-8')
        return {"question": question.question, "answer": cached_value}

    logger.debug(f"Cache miss for question: {question.question}")

    #TODO: implement calls to NATS and other services to get the answer

    # Step 1: Query the database service
    try:
        logger.debug(f"Requesting query on NATS topic {db_query_topic}")
        db_request_payload = json.dumps({"question": question.question}).encode()
        db_answer = await nats_client.request(db_query_topic, db_request_payload, timeout=30)
        
        data = db_answer.data.decode()
        preview = data[:300] + "..." if len(data) > 300 else data
        logger.debug(f"Received DB answer: {preview}")
    except Exception as e:
        logger.error(f"Failed to retrieve data from db service: {e}")
        raise HTTPException(status_code=500, detail="Failed to query database")

    # Step 2: Generate natural language answer
    try:
        logger.debug(f"Requesting answer on NATS topic {answer_topic}")
        question_answer = await nats_client.request(answer_topic, data.encode(), timeout=30)
        
        answer = question_answer.data.decode('utf-8')
        preview = answer[:300] + "..." if len(answer) > 300 else answer
        logger.debug(f"Received answer: {preview}")
    except Exception as e:
        logger.error(f"Failed to generate answer: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate answer")

    # Step 3: Cache the answer (encode as UTF-8 bytes for memcached)
    try:
        # Store the answer as UTF-8 encoded bytes to handle non-ASCII characters
        mem_client.set(cache_key, answer.encode('utf-8'))
        logger.debug(f"Cached answer for question hash: {cache_key[:16]}...")
    except Exception as e:
        # Log but don't fail if caching fails
        logger.warning(f"Failed to cache answer: {e}")

    return {"question": question.question, "answer": answer}
