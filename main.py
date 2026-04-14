import os
import shutil
import logging
from datetime import datetime

from extract import extract_from_csv
from transform import transform, reject, aggregate
from load import load_to_csv
import config


def run_pipeline(run_date: str = None, force: bool = False):
    logging.info("Pipeline started")

    # Use date partition (idempotency key)
    run_date = run_date or datetime.today().strftime("%Y-%m-%d")

    final_path = os.path.join(config.BASE_OUTPUT, f"date={run_date}")
    tmp_path = os.path.join(config.BASE_OUTPUT, f"_tmp_{run_date}")

    # Skip if already processed (idempotent behavior)
    if os.path.exists(final_path) and not force:
        logging.info(f"Data for {run_date} already exists. Skipping run.")
        return

    # Clean temp if exists
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)
    os.makedirs(tmp_path, exist_ok=True)

    try:
        # Extract
        orders_raw = extract_from_csv(config.INPUT_FILE)
        ref_raw = extract_from_csv(config.REF_FILE)

        # Transform
        db = transform(orders_raw, ref_raw)
        db_clean, db_rejected = reject(db)
        aggregated_data = aggregate(db_clean)

        # Load to TEMP (never directly to final)
        load_to_csv(db_clean, os.path.join(tmp_path, "clean.csv"))
        load_to_csv(db_rejected, os.path.join(tmp_path, "rejected.csv"))
        load_to_csv(aggregated_data, os.path.join(tmp_path, "aggregated.csv"))

        # Atomic move: temp → final
        if os.path.exists(final_path):
            shutil.rmtree(final_path)

        os.rename(tmp_path, final_path)
        logging.info(f"Data successfully written to {final_path}")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        # Clean up temp on failure
        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)
        raise

    logging.info("Pipeline finished")


if __name__ == "__main__":
    run_pipeline()