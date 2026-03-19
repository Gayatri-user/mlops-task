FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install pandas numpy pyyaml

CMD ["python", "run.py", "--input", "data.csv", "--config", "config.yaml", "--output", "metrics.json", "--log-file", "run.log"]