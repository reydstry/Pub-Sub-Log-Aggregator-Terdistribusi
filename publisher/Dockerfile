FROM python:3.11-slim

# Desain: non-root user untuk keamanan, sama seperti aggregator.
# Publisher adalah one-shot script (jalankan sekali lalu exit).

LABEL maintainer="publisher-service"
LABEL description="Pub-Sub Log Aggregator - Event Publisher Simulator"

# Buat non-root user
RUN groupadd --gid 1000 appuser \
 && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown -R appuser:appuser /app
USER appuser

CMD ["python", "-u", "publisher.py"]
