FROM python:3.11-slim

WORKDIR /app

COPY producer_simulator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer_simulator/ .

CMD ["python", "simulator.py"]