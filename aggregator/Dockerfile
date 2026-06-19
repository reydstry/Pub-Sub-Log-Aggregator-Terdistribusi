FROM python:3.11-slim

# Desain: non-root user "appuser" untuk keamanan —
# menghindari proses berjalan sebagai root di dalam container.

LABEL maintainer="aggregator-service"
LABEL description="Pub-Sub Log Aggregator - FastAPI Consumer Service"

# Buat non-root user
RUN groupadd --gid 1000 appuser \
 && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

# Install dependencies dulu (layer caching: requirements jarang berubah)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin seluruh source code
COPY . .

# Ganti ke non-root user sebelum CMD
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
