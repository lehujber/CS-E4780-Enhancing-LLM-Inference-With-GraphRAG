FROM python:3.12-slim AS app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["python", "-m", "src.main"]

FROM app AS perf

CMD ["python", "-m", "cProfile", "-o", "/perf/query-service-profile.prof", "-m", "src.main"]