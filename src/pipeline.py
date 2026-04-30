from config.config import load_config
from src.ETL import extract_data, transform_data, load_data
from src.logger import setup_logger
from src.generate_synt_data import generate_balanced_credit_data
from src.clean_script import clean_and_standardize

import os

def run_pipeline():
    logger = setup_logger()
    config = load_config()

    raw_path = config["data"]["raw_path"]
    db_path = config["data"]["db_path"]
    data_proc_dir = config["directories"]["data_proc_dir"]
    table_name = config["data"]["table_name"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    os.makedirs(os.path.dirname(data_proc_dir), exist_ok=True)

    # ETL
    try:
        logger.info("Starting ETL pipeline")
        logger.info(f"Raw path: {raw_path}")
        logger.info(f"Database path: {db_path}")
        logger.info(f"Table name: {table_name}")

        df = extract_data(raw_path, logger=logger)
        logger.info(f"Extracted data shape: {df.shape}")

        df_transformed = transform_data(df, logger=logger)
        logger.info(f"Transformed data shape: {df_transformed.shape}")

        load_data(df_transformed, db_path, table_name, logger=logger)
        logger.info("Data loaded successfully into SQLite")

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        raise

    # Generating synthetic data
    try:
        logger.info("Starting generating synthetic data pipeline")
        df_balanced, trained_model = generate_balanced_credit_data(df_transformed, logger=logger)
        logger.info("Balance achieved:")
        logger.info(df_balanced['default'].value_counts())
        logger.info("Generating synthetic data pipeline completed successfully")

    except Exception as e:
        logger.error(f"Generating synthetic data pipeline failed: {e}", exc_info=True)
        raise

    # Cleaning data
    try:
        logger.info("Starting cleaning data pipeline")
        
        df_final = clean_and_standardize(df_balanced, logger=logger)
        logger.info("Cleaning data pipeline completed successfully")

    except Exception as e:
        logger.error(f"Cleaning data pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()