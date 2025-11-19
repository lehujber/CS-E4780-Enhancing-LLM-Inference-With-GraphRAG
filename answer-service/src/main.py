import asyncio
import json
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg as NATSMsg

import dspy

from .config import (
    NATS_HOST,
    NATS_PORT,
    NATS_ANSWER_TOPIC as topic,
    OPENROUTER_API_KEY,
    get_logger
)

logger = get_logger("main")

# Same DSPy configuration of query-service
lm = dspy.LM(
    "openrouter/google/gemini-2.0-flash-001",
    api_key=OPENROUTER_API_KEY,
)
dspy.configure(lm=lm)

# DSPy Signature for answer generation
class AnswerQuestion(dspy.Signature):
    """
    - Use the provided question, the generated Cypher query and the context to answer the question.
    - If the context is empty, state that you don't have enough information to answer the question.
    - When dealing with dates, mention the month in full.
    """
    question: str = dspy.InputField()
    cypher_query: str = dspy.InputField()
    context: str = dspy.InputField()
    response: str = dspy.OutputField()

# Create the answer generator module
answer_generator = dspy.ChainOfThought(AnswerQuestion)

async def message_handler(msg: NATSMsg):
    try:
        parsed_data = json.loads(msg.data.decode())
        logger.debug(
            f"Received a message on '{msg.subject}': \n"
            f" question: {parsed_data['question']} \n"
            f" cypher: {parsed_data['cypher']} \n"
            f" columns: {parsed_data['columns']} \n"
            f" rows: {len(parsed_data['rows'])}"
        )

        question = parsed_data.get("question", "")
        cypher = parsed_data.get("cypher", "")
        rows = parsed_data.get("rows", [])

        # Format the results as context for the LLM
        # Following the professor codebase: flatten rows into a simple list
        context = str([item for row in rows for item in row])

        result = answer_generator(
            question=question,
            cypher_query=cypher,
            context=context
        )
        answer = result.response

        logger.info(f"Generated answer for: '{question}'")
        await msg.respond(answer.encode("utf-8"))

    except Exception as e:
        logger.error(f"Error in message handler: {e}")
        error_msg = "Sorry, I encountered an error while generating the answer."
        await msg.respond(error_msg.encode("utf-8"))

async def main():
    logger.info("Starting Answer Service...")


    nc = NATS()
    await nc.connect(f"nats://{NATS_HOST}:{NATS_PORT}")
    await nc.subscribe(topic, cb=message_handler)

    logger.debug(f"Subscribed to topic '{topic}' on NATS server at {NATS_HOST}:{NATS_PORT}")

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())