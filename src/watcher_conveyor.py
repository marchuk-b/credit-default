import os
import time
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config.config import load_config
from src.ETL import extract_data, transform_data, load_data
from src.logger import setup_logger
from src.generate_synt_data import generate_balanced_credit_data
from src.clean_script import clean_and_standardize

class DataPipelineHandler(FileSystemEventHandler):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.db_path = config["data"]["db_path"]
        self.data_proc_dir = config["directories"]["data_proc_dir"]
        self.archive_dir = config["directories"]["archive_dir"]
        self.input_dir = config["directories"]["input_dir"]

    def on_created(self, event):
        # Listen only Excel files
        if event.is_directory or not event.src_path.endswith(('.xls', '.xlsx')):
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)
        self.logger.info(f"Found a new file for processing: {file_name}")
        start_time = time.time()
        
        try:
            # 1. INGESTION & VALIDATION
            self.logger.info("Step 1: Reading and validation...")
            # extract_data already include pd.read_excel and validate_raw_data
            df_raw = extract_data(file_path, logger=self.logger) 
            
            # 2. TRANSFORMATION
            self.logger.info("Step 2: ETL Transform...")
            df_transformed = transform_data(df_raw, logger=self.logger)

            self.logger.info("Step 3: Generating of synthetic data (CTGAN)...")
            df_balanced, trained_model = generate_balanced_credit_data(df_transformed, logger=self.logger)

            self.logger.info("Step 4: Clearing and standardization...")
            df_final = clean_and_standardize(df_balanced, logger=self.logger)

            # 3. STORAGE
            self.logger.info("Step 5: Saving results...")
            
            # Saving in SQLite
            load_data(df_raw, self.db_path, "raw_credit_data", logger=self.logger)
            load_data(df_balanced, self.db_path, "balanced_credit_data", logger=self.logger)
            load_data(df_final, self.db_path, "cleaned_credit_data", logger=self.logger)

            # Saving final CSV
            csv_name = file_name.replace('.xlsx', '.csv').replace('.xls', '.csv')
            processed_csv_path = os.path.join(self.data_proc_dir, f"cleaned_{csv_name}")
            df_final.to_csv(processed_csv_path, index=False)
            self.logger.info(f"CSV saved: {processed_csv_path}")

            # 4. Archivation
            os.makedirs(self.archive_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(self.archive_dir, file_name))
            
            processing_time = time.time() - start_time
            self.logger.info(f"The conveyor successfully completed task in {processing_time:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Critical error: {e}", exc_info=True)


def start_watcher():
    logger = setup_logger()
    config = load_config()
    
    input_dir = config["directories"]["input_dir"]
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(config["directories"]["data_proc_dir"], exist_ok=True)
    os.makedirs(config["directories"]["archive_dir"], exist_ok=True)
    os.makedirs(os.path.dirname(config["data"]["db_path"]), exist_ok=True) 

    event_handler = DataPipelineHandler(config, logger)
    observer = Observer()
    observer.schedule(event_handler, path=input_dir, recursive=False)
    
    logger.info(f"Watchdog is active. Waiting on new files in: {input_dir}")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Conveyor is stopped!!!")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()