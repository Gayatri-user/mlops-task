# MLOps Task 0

## Overview
This project implements a reproducible, observable, and Dockerized ML batch pipeline.

## Run Locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

## Run with Docker
docker build -t mlops-task .
docker run --rm mlops-task

## Output Example
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4989,
  "latency_ms": 25,
  "seed": 42,
  "status": "success"
}
