import pandas as pd
import numpy as np
import joblib
import sqlite3
import logging
import os
from datetime import datetime
from config.config import load_config

class MLInferenceService:
    def __init__(self, model_path, db_path, output_csv_path):
        self.model_path = model_path
        self.db_path = db_path
        self.output_csv_path = output_csv_path
        self.model_version = "v1.0-xgboost-optimized"
        self.logger = self._setup_logger()
        self.model = None

    def _setup_logger(self):
        logger = logging.getLogger("ML_Inference")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            # Handler for printing in console
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(ch)
            
            # 2. NEW: Handler for writing in a file
            os.makedirs("logs", exist_ok=True)
            fh = logging.FileHandler("logs/inference.log", encoding='utf-8')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            
        return logger

    def load_model(self):
        try:
            self.logger.info(f"Loading model from {self.model_path}...")
            self.model = joblib.load(self.model_path)
            self.logger.info("Model is loaded successfully!")
        except Exception as e:
            self.logger.error(f"Error: {e}")
            raise

    def process_batch(self, data_path):
        try:
            self.logger.info(f"Reading new input data from {data_path}...")
            df = pd.read_csv(data_path)
            
            # Falsifying a customer ID (which is actually an account or passport number)
            if 'client_id' not in df.columns:
                df['client_id'] = [f"CLI_{i:05d}" for i in range(len(df))]
            
            client_ids = df['client_id']
            
            # Remove the target variable (if it was accidentally included from the test file) and the model ID
            X_new = df.drop(columns=['default', 'client_id'], errors='ignore')
            
            self.logger.info("Performing inference...")
            # Get classes (0 or 1)
            predictions = self.model.predict(X_new)
            # Get probability of default (from 0.0 to 1.0)
            probabilities = self.model.predict_proba(X_new)[:, 1] 
            
            # Achieving business results
            results_df = pd.DataFrame({
                'client_id': client_ids,
                'prediction_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'predicted_class': predictions,
                'default_probability': np.round(probabilities, 4),
                # Classification by risk zone according to business logic
                'risk_zone': ['High Risk (Rejection)' if p > 0.6 else ('Medium Risk' if p > 0.3 else 'Low Risk (Approved)') for p in probabilities],
                'model_version': self.model_version
            })
            
            self.logger.info("Forecasting is complete. Saving results...")
            self.save_results(results_df)
            
            return results_df
            
        except Exception as e:
            self.logger.error(f"Critical error while processing the data packet: {e}")
            raise

    def save_results(self, results_df):
        # Save in CSV
        os.makedirs(os.path.dirname(self.output_csv_path), exist_ok=True)
        results_df.to_csv(self.output_csv_path, index=False)
        self.logger.info(f"Results saved in file: {self.output_csv_path}")
        
        # Save in SQLite
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        # Add results in table (if_exists='append' add new data to already existing)
        results_df.to_sql('client_scoring_results', conn, if_exists='append', index=False)
        conn.close()
        self.logger.info(f"Results successfully in DB: {self.db_path} (Table: client_scoring_results)")

if __name__ == "__main__":
    config = load_config()
    MODEL_PATH = "artifacts/xgboost_credit_20260506_232415/model.pkl" 
    
    # For testing give our clean data
    NEW_DATA_PATH = config["data"]["cleaned_path"] 
    
    DB_OUTPUT_PATH = config["data"]["business_results"]
    CSV_OUTPUT_PATH = config["data"]["predictions"]
    
    # Initialisation and launch of the service
    service = MLInferenceService(MODEL_PATH, DB_OUTPUT_PATH, CSV_OUTPUT_PATH)
    service.load_model()
    final_results = service.process_batch(NEW_DATA_PATH)
    
    print("\n--- Example of source business data (the first 5 customers) ---")
    print(final_results.head().to_markdown(index=False))