# CS-E4780-Enhancing-LLM-Inference-With-GraphRAG
Course project 2 for CS-E4780 at Aalto university

## Running the application:
```
docker compose --env-file .composeEnv up --build
```

## Running the application in benchmark mode:
```
docker compose --file docker-compose-benchmark.yaml --env-file .composeEnv up --build
```

## Submitting a question:
```
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"question":"sample_question"}' \
  http://localhost:8000/question
```