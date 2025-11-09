FROM python:3.11-slim

WORKDIR /app

# Install lightweight runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src ./src

# Set the default command
CMD ["python", "-m", "src.main"]
