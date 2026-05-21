import os
import time
import shutil
import logging
import joblib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config.config import load_config
from src.data.ETL import extract_data, transform_data
from src.utils.logger import setup_logger
from src.models.inference import MLInferenceService

class MainPipelineHandler(FileSystemEventHandler):
    def __init__(self, logger: logging.Logger, config: dict):
        self.logger = logger
        self.config = config
        self.archive_dir = config["directories"]["archive_dir"]
        self.data_proc_dir = config["directories"]["data_proc_dir"]
        
        self.model_path = config["ml"]["working_model_path"]
        self.scaler_path = config["ml"]["working_scaler_path"]
        self.db_output = config["database"]["business_results"]
        self.csv_output = config["data"]["predictions"]
        
        # Initialization ML-service
        self.logger.info("Initialization ML Inference Service...")
        self.inference_service = MLInferenceService(self.model_path, self.db_output, self.csv_output)
        self.inference_service.load_model()
        
        # Load StandardScaler
        self.logger.info(f"Loading of StandardScaler from {self.scaler_path}...")
        self.scaler = joblib.load(self.scaler_path)

    def on_created(self, event) -> None:
        # Listen files Excel and CSV
        if event.is_directory or not event.src_path.endswith(('.xls', '.xlsx', '.csv')):
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)
        self.logger.info(f"--- FOUND NEW FILE: {file_name} ---")
        start_time = time.time()
        
        try:
            # Step 1: Reading and validation
            self.logger.info("Step 1: Reading and validation data...")
            df_raw = extract_data(file_path, logger=self.logger) 
            
            # Step 2: Basic transformation
            self.logger.info("Step 2: Basic transformation...")
            df_transformed = transform_data(df_raw, logger=self.logger)

            # Step 3: Scaling
            self.logger.info("Step 3: Normalising the values...")
            cols_to_scale = ['limit_bal', 'age', 'total_bill', 'total_paid']
            
            df_transformed[cols_to_scale] = self.scaler.transform(df_transformed[cols_to_scale])
            
            # Save temp CSV for Inference Service
            temp_csv_path = os.path.join(self.data_proc_dir, f"ready_for_inference_{file_name}.csv")
            df_transformed.to_csv(temp_csv_path, index=False)
            
            # Step 4: Prediction (Inference)
            self.logger.info("Step 4: Launch of ML-based risk forecasting...")
            self.inference_service.process_batch(temp_csv_path)

            # Step 5: Archiving
            self.logger.info("Step 5: Archive the source file and clear...")
            os.makedirs(self.archive_dir, exist_ok=True)
            
            archive_path = os.path.join(self.archive_dir, file_name)
            if os.path.exists(archive_path):
                os.remove(archive_path)
                
            shutil.move(file_path, archive_path)
            
            # Delete temp file
            if os.path.exists(temp_csv_path):
                os.remove(temp_csv_path)
            
            processing_time = time.time() - start_time
            self.logger.info(f"SUCCESS: The pipeline processed {file_name} in {processing_time:.2f} seconds.\n")

        except Exception as e:
            self.logger.error(f"A critical error occurred during processing {file_name}: {e}", exc_info=True)
            # Rename the file to avoid an endless loop of errors
            error_path = file_path + ".error"
            if os.path.exists(file_path):
                os.rename(file_path, error_path)
            self.logger.warning(f"The file has been renamed to {file_name}.error for manual analysis.")


def start_watcher() -> None:
    logger = setup_logger(name="Watcher", log_file="watcher.log")
    config = load_config()

    INPUT_DIR = config["directories"]["input_dir"]
    
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(config["directories"]["archive_dir"], exist_ok=True)
    os.makedirs(config["directories"]["data_proc_dir"], exist_ok=True)
    os.makedirs(config["directories"]["artifacts_dir"], exist_ok=True)

    event_handler = MainPipelineHandler(logger, config)
    observer = Observer()
    observer.schedule(event_handler, path=INPUT_DIR, recursive=False)
    
    logger.info(f"Watchdog ACTIVE. Waiting for new files in the directory: {INPUT_DIR}")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("The conveyor has been stopped by the user.")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()