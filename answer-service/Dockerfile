FROM python:3.14-alpine AS app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["python", "-m", "src.main"]

FROM app AS perf

RUN pip3 install --no-cache-dir flameprof

CMD ["python", "-m","cProfile", "-o", "/perf/answer-service-profile.prof", "-m", "src.main"]