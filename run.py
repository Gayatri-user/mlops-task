import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys

# Logging setup
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

# Error handling
def write_error(output, version, message):
    error_data = {
        "version": version if version else "unknown",
        "status": "error",
        "error_message": message
    }
    with open(output, "w") as f:
        json.dump(error_data, f, indent=2)

    print(json.dumps(error_data, indent=2))
    sys.exit(1)

# Main function
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("Job started")

    start_time = time.time()

    # Load config
    try:
        with open(args.config) as f:
            config = yaml.safe_load(f)

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

    except Exception as e:
        write_error(args.output, None, f"Config error: {str(e)}")

    # Load data (manual parsing)
    try:
        with open(args.input, "r") as f:
            lines = f.readlines()

        lines = [line.strip() for line in lines]

        columns = lines[0].split(",")
        data = [line.split(",") for line in lines[1:]]

        df = pd.DataFrame(data, columns=columns)

        if df.empty:
            raise ValueError("Dataset is empty")

        if "close" not in df.columns:
            raise ValueError("Missing 'close' column")

        df["close"] = df["close"].astype(float)

        logging.info(f"Rows loaded: {len(df)}")

    except Exception as e:
        write_error(args.output, version, f"Data error: {str(e)}")

    # Processing
    try:
        logging.info("Computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        logging.info("Generating signal")
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        signal_rate = df["signal"].mean()

    except Exception as e:
        write_error(args.output, version, f"Processing error: {str(e)}")

    # Metrics
    latency_ms = int((time.time() - start_time) * 1000)

    result = {
        "version": version,
        "rows_processed": int(len(df)),
        "metric": "signal_rate",
        "value": float(round(signal_rate, 4)),
        "latency_ms": latency_ms,
        "seed": seed,
        "status": "success"
    }

    # Save output
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(result, indent=2))

    logging.info(f"Metrics: {result}")
    logging.info("Job completed successfully")


if __name__ == "__main__":
    main()