from extract import extract_from_csv
from transform import transform
from transform import reject
from load import load_to_csv
import pandas as pd
import logging
import config

#url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
def run_pipeline():
    logging.info("Pipeline started")
    # Extract
    logging.info("Extracting orders & reference DBs")
    orders_raw = extract_from_csv(config.INPUT_FILE)
    ref_raw = extract_from_csv(config.REF_FILE)
    logging.info("DBs extracted successfully")

    # Transform
    logging.info("Transforming raw orders list")
    db = transform(orders_raw, ref_raw)
    
    logging.info("Cleaning rejected rows into their own database")
    db_clean, db_rejected = reject(db)
    logging.info("Transform stage complete")

    # Load
    load_to_csv(db_clean, config.OUTPUT_CLEAN)
    load_to_csv(db_rejected, config.OUTPUT_REJECT)
    logging.info("Data saved to .csv")

if __name__ == "__main__":
    run_pipeline()
    logging.info("Pipeline finished")