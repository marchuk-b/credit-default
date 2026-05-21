import os
import time
from config.config import load_config
from src.utils.logger import setup_logger
from src.data.ETL import extract_data, transform_data, load_data
from src.data.generate_synt_data import generate_balanced_credit_data
from src.data.clean_script import clean_and_standardize
from src.models.ml_pipeline import CreditScoringPipeline

def run_training_pipeline() -> None:
    # Initialization
    logger = setup_logger(name="TrainingPipeline", log_file="training_pipeline.log")
    config = load_config()
    
    logger.info("=== LAUNCHING THE MODEL TRAINING PIPELINE ===")
    start_time = time.time()
    
    try:
        # Extract & Transform (ETL)
        raw_path = config["data"]["test_data"]
        logger.info("Stage 1: Loading and basic transformation of historical data...")
        df_raw = extract_data(raw_path, logger=logger)
        df_transformed = transform_data(df_raw, logger=logger)
        
        # Balancing (CTGAN)
        logger.info("Stage 2: Class balancing (CTGAN) – this may take some time...")
        df_balanced, _ = generate_balanced_credit_data(df_transformed, logger=logger)
        
        # Cleaning & Scaling (Create a scaler.pkl)
        logger.info("Stage 3: Removing anomalies and scaling...")
        df_cleaned, scaler = clean_and_standardize(df_balanced, logger=logger)
        
        # Save cleaned dataset for model
        cleaned_data_path = config["data"]["cleaned_data"]
        os.makedirs(os.path.dirname(cleaned_data_path), exist_ok=True)
        df_cleaned.to_csv(cleaned_data_path, index=False)
        logger.info(f"Cleaned data saved in: {cleaned_data_path}")
        
        # Training ML Model
        logger.info("Step 4: Training the XGBoost model...")
        artifacts_dir = config["directories"]["artifacts_dir"]
        model_name = config["ml"]["model_name"]
        
        ml_pipeline = CreditScoringPipeline(data_path=cleaned_data_path, 
                                            base_output_dir=artifacts_dir, 
                                            model_name=model_name)
        ml_pipeline.run()

        scaler_path = os.path.join(ml_pipeline.run_dir, "scaler.pkl")
        import joblib
        joblib.dump(scaler, scaler_path)
        logger.info(f"Scaler and Model successfully bundled in: {ml_pipeline.run_dir}")
        
        # End
        total_time = time.time() - start_time
        logger.info(f"=== TRAINING SUCCESSFULLY COMPLETED IN {total_time:.2f} SECONDS! ===")
        logger.info("The artefacts (scaler.pkl, model.pkl, report.json) are ready for use in the Watcher pipeline.")
        
    except Exception as e:
        logger.critical(f"Training pipeline error: {e}", exc_info=True)

if __name__ == "__main__":
    run_training_pipeline()