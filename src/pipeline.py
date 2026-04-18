from config.config import load_config
from src.ETL import extract_data, transform_data, load_data
from src.logger import setup_logger

import os


def run_pipeline():
    logger = setup_logger()
    config = load_config()

    raw_path = config["data"]["raw_path"]
    db_path = config["data"]["db_path"]
    table_name = config["data"]["table_name"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    try:
        logger.info("Starting ETL pipeline")
        logger.info(f"Raw path: {raw_path}")
        logger.info(f"Database path: {db_path}")
        logger.info(f"Table name: {table_name}")

        df = extract_data(raw_path)
        logger.info(f"Extracted data shape: {df.shape}")

        df_transformed = transform_data(df)
        logger.info(f"Transformed data shape: {df_transformed.shape}")

        load_data(df_transformed, db_path, table_name)
        logger.info("Data loaded successfully into SQLite")

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()