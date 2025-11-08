from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

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

    cached_value = mem_client.get(question.question)
    if cached_value:
        logger.debug(f"Cache hit for question: {question.question}")
        return {"question": question.question, "answer": cached_value}

    logger.debug(f"Cache miss for question: {question.question}")

    #TODO: implement calls to NATS and other services to get the answer

    try:
        logger.debug(f"Requested question on NATS topic {db_query_topic}")
        db_answer = await nats_client.request(db_query_topic, question.question.encode())

        data = db_answer.data.decode()  
        logger.debug(f"Received DB answer: {data}")

        logger.debug(f"Requested answer on NATS topic {answer_topic}")
        question_answer = await nats_client.request(answer_topic, data.encode())
    
        answer = question_answer.data.decode()

        mem_client.set(question.question, answer)

        return {"question": question.question, "answer": answer}
    except Exception as e:
        logger.error(f"Failed to retrieve data from db service: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
