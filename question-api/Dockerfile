FROM python:3.14-alpine AS app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM app AS perf

CMD ["python", "-m", "cProfile", "-o", "/perf/question-api-profile.prof", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]