from config.config import load_config
from src.ETL import extract_data, transform_data, load_data
from src.logger import setup_logger
from src.generate_synt_data import generate_balanced_credit_data
from src.clean_script import clean_and_standardize

import os
import shutil

def run_pipeline():
    logger = setup_logger()
    config = load_config()

    raw_path = config["data"]["raw_path"]
    db_path = config["data"]["db_path"]
    data_proc_dir = config["directories"]["data_proc_dir"]
    archive_dir = config["directories"]["archive_dir"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    os.makedirs(data_proc_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # ETL
    try:
        logger.info("Starting ETL pipeline")
        logger.info(f"Raw path: {raw_path}")
        logger.info(f"Database path: {db_path}")

        df_raw = extract_data(raw_path, logger=logger)
        logger.info(f"Extracted data shape: {df_raw.shape}")

        df_transformed = transform_data(df_raw, logger=logger)
        logger.info(f"Transformed data shape: {df_transformed.shape}")

        # Збереження сирих даних у першу таблицю
        load_data(df_raw, db_path, "raw_credit_data", logger=logger)

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
        
        # Saving balanced data in db
        load_data(df_balanced, db_path, "balanced_credit_data", logger=logger)
        
        logger.info("Generating synthetic data pipeline completed successfully")

    except Exception as e:
        logger.error(f"Generating synthetic data pipeline failed: {e}", exc_info=True)
        raise

    # Cleaning data & Archiving
    try:
        logger.info("Starting cleaning data pipeline")
        
        df_final = clean_and_standardize(df_balanced, logger=logger)
        
        # Saving cleared data in db
        load_data(df_final, db_path, "cleaned_credit_data", logger=logger)
        logger.info("Cleaning data pipeline completed successfully")

        # Saving final CSV
        file_name = os.path.basename(raw_path)
        csv_name = file_name.replace('.xlsx', '.csv').replace('.xls', '.csv')
        processed_csv_path = os.path.join(data_proc_dir, f"cleaned_{csv_name}")
        df_final.to_csv(processed_csv_path, index=False)
        logger.info(f"Final CSV saved to: {processed_csv_path}")

        # Archiving of start file
        archive_path = os.path.join(archive_dir, file_name)
        shutil.move(raw_path, archive_path)
        logger.info(f"Original file successfully archived to: {archive_path}")

    except Exception as e:
        logger.error(f"Cleaning data or Archiving failed: {e}", exc_info=True)
        raise

    logger.info("Full pipeline executed successfully.")

if __name__ == "__main__":
    run_pipeline()