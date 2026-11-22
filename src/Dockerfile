FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY src/simulator.py /app/src/simulator.py
COPY src/ingest.py /app/src/ingest.py
COPY src/__init__.py /app/src/__init__.py

RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["python", "-m", "src.simulator"]
