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

questions = [
    "Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
    "Which scholars were born in cities located in Germany?",
    "Which institutions are located in cities in the United States?",
    "Which scholars won a prize in Chemistry after 1950?",
    "Which scholars were awarded prizes with the motivation containing the word 'peace'?",
    "Which prizes were awarded before 1920 in the category of Literature?",
    "Which scholars died in cities located in France?",
    "List all scholars who were affiliated with institutions in Sweden.",
    "Which scholars won multiple prizes?",
    "Which cities are located in countries within the continent of Asia?",
    "Which scholars were born in the same city where they later died?",
    "Which scholars won a prize amount adjusted greater than 1,000,000?",
    "Which institutions are in cities located in the continent of Europe?",
    "Which scholars won prizes in the category of Medicine?",
    "Which scholars were affiliated with institutions located in London?",
    "Which scholars won prizes in Economics in the year 2000?",
    "Which scholars of type 'individual' won a Physics prize?",
    "Which scholars were born in cities located in the United Kingdom?",
    "Which scholars won prizes with a motivation referencing ‘discovery’?",
    "Which prizes were awarded in years before the scholar’s death date?",
    "Which scholars were affiliated with more than one institution?",
    "Which scholars were born in cities located in countries within the continent of Africa?",
    "Which prizes had a prizeAmount less than 500000?",
    "Which categories of prizes were awarded in 2010?",
    "Which scholars won prizes in the category of Peace?",
    "Which scholars had the gender 'female' and won a prize in Chemistry?",
    "Which scholars were affiliated with institutions located in Tokyo?",
    "Which countries in Europe have cities that are birthplaces of prize winners?",
    "Which scholars won prizes in the same year they were affiliated with a specific institution?",
    "Which prizes were awarded in the earliest awardYear recorded?",
    "Which scholars were born in cities located in the continent of South America?",
    "Which scholars died before receiving their prize?",
    "Which institutions are located in cities that belong to countries within the continent of North America?",
    "Which scholars with scholar_type 'organization' won a Peace prize?",
    "Which scholars won a prize in Mathematics (if any exist in the dataset)?",
    "Which scholars were affiliated with Harvard University?",
    "Which scholars died in cities located in Asia?",
    "Which scholars won the highest adjusted prize amount in Physics?",
    "Which cities are birthplaces of winners of the Literature prize?",
    "Which scholars won prizes in years after 2000?",
    "Which scholars won prizes in the same category more than once?",
    "Which scholars were born in cities located in Australia?",
    "Which scholars were affiliated with institutions located outside their birth country?",
    "Which scholars had a death date but never won any prize?",
    "Which prizes were awarded in categories that start with the letter 'P'?",
    "Which scholars were associated with institutions in Berlin?",
    "Which scholars won prizes that had the motivation containing the word 'development'?",
    "Which scholars were born in New York City?",
    "Which scholars won prizes in prize categories with adjusted amounts over 5,000,000?",
    "Which scholars had unknown or empty deathDate values?",
    "Which cities have institutions affiliated with Nobel laureates?",
    "Which scholars won their prize in the earliest awardYear available?",
    "Which scholars died in the same city where they were affiliated at some point?",
    "Which scholars born in France won a prize in Literature?",
    "Which gender is most represented among the Nobel Prize winners in Chemistry?",
    "Which scholars won prizes with a portion marked as '1/2'?",
    "Which scholars were affiliated with institutions located in cities belonging to the continent of Europe?",
    "Which institutions are located in cities within countries that belong to the continent of Oceania?",
    "Which scholars won prizes for work related to climate research?",
    "Which prizes were awarded on the latest dateAwarded available?",
    "Which cities are death places of Physics prize winners?",
    "Which scholars affiliated with MIT won prizes in Physics?",
    "Which scholars won prizes in Medicine with motivations referencing ‘RNA’?",
    "Which scholars won prizes in categories they are not traditionally associated with?",
    "Which scholars were born in countries that are part of the continent of Asia?",
    "Which scholars won prizes in any category in 1995?",
    "Which scholars won prizes while affiliated with the same institution where they were born?",
    "Which prizes had the largest portion values assigned?",
    "Which scholars were born in capital cities?",
    "Which scholars affiliated with Oxford University won any prize?",
    "Which scholars won prizes in Chemistry in the 1970s?",
    "Which scholars died in cities located in Canada?",
    "Which scholars were born in cities in countries within the continent of Europe and won prizes in Literature?",
    "Which institutions located in Paris have affiliated prize winners?",
    "Which prizes awarded in the category of Peace have prizeAmountAdjusted less than 500000?",
    "Which scholars who won in Physics were born in the same continent where the prize was awarded?",
    "Which scholars have the knownName field empty or missing?",
    "Which scholars were awarded prizes with motivations containing the word 'human rights'?",
    "Which scholars won prizes in categories that were newly introduced?",
    "Which cities are both birth and affiliate locations for scholars?",
    "Which scholars affiliated with ETH Zurich won prizes?",
    "Which scholars won prizes in Economics with motivations referencing ‘market’ or ‘economics’?",
    "Which scholars were born in cities located in countries within the continent of North America and won Peace prizes?",
    "Which categories had the highest total prizeAmount awarded in a single year?",
    "Which scholars were born and died on the same date?",
    "Which scholars won prizes with portion equal to '1/3'?",
    "Which scholars were awarded prizes in years that match their birthDate year?",
    "Which scholars won prizes but were never affiliated with any institution?",
    "Which scholars who won Literature prizes died in the United Kingdom?",
    "Which institutions are located in cities that are also birthplaces of Chemistry prize winners?",
    "Which scholars who won Peace prizes were affiliated with institutions in Europe?",
    "Which prizes had motivations referencing breakthroughs in physics or astronomy?",
    "Which scholars born in Sweden won prizes in Medicine?",
    "Which scholars died in cities located in the continent of South America?",
    "Which scholars affiliated with institutions in China won prizes?",
    "Which prize categories had no winners in certain years?",
    "Which scholars born in Asia later won prizes in Economics?",
    "Which scholars won prizes with motivations referencing ‘quantum’?"
]

async def main():
    await asyncio.sleep(10)

    results_end_to_end = []
    for q in questions:
        result = await benchmark_end_to_end(q)
        results_end_to_end.append(result)

    total_request_delay = sum(res["timings"]["total_request_time_ms"] for res in results_end_to_end)
    with open("/benchmark-data/benchmark_end_to_end_results.json", "w") as f:
        output = {
            "results": results_end_to_end,
            "total_request_delay_ms": total_request_delay,
        }
        json.dump(output, f, indent=4)

async def benchmark_end_to_end(question: str) -> dict:
    request_start = datetime.datetime.now()
    response = reqs.post(API_ENDPOINT_URL, json={"question": question}, timeout=30)
    request_end = datetime.datetime.now()

    response_json = response.json()
    
    return {
        "question": question,
        "answer": response_json.get("answer", ""),
        "timings": {
            "total_request_time_ms": (request_end - request_start).total_seconds() * 1000,
            **response_json.get("timings", {})
        }
    }

if __name__ == "__main__":
    asyncio.run(main())