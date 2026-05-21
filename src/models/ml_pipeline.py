import pandas as pd
import json
import joblib
import logging
import os
from datetime import datetime

from config.config import load_config
from src.utils.logger import setup_logger

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from xgboost import XGBClassifier

class CreditScoringPipeline:
    def __init__(self, data_path: str, base_output_dir: str, model_name: str):
        self.data_path = data_path
        self.base_output_dir = base_output_dir
        self.model_name = model_name
        self.logger = setup_logger(name="ML_Pipeline", log_file="pipeline.log")
        
        # Generating a unique folder name based on the time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = os.path.join(self.base_output_dir, f"{self.model_name}_{timestamp}")
        
        # Paths to files within this unique folder
        self.model_save_path = os.path.join(self.run_dir, "model.pkl")
        self.report_save_path = os.path.join(self.run_dir, "report.json")
        
        self.report = {"execution_time": str(datetime.now())}

    def load_data(self) -> None:
        self.logger.info("Loading data...")
        self.df = pd.read_csv(self.data_path)
        self.report["dataset"] = os.path.basename(self.data_path)
        self.report["data_shape"] = self.df.shape
        self.logger.info(f"Loaded {self.df.shape[0]} records.")

    def check_data_quality(self) -> None:
        self.logger.info("Automated Data Quality Control...")
        
        # Missing values
        missing_ratios = self.df.isnull().mean().to_dict()
        total_missing = sum(missing_ratios.values())
        
        # Duplicates
        duplicates = self.df.duplicated().sum()
        
        # Class balance
        class_balance = self.df['default'].value_counts(normalize=True).to_dict()

        dq_stats = {
            "total_missing_values": total_missing,
            "duplicates": int(duplicates),
            "class_balance_0": round(class_balance.get(0, 0), 2),
            "class_balance_1": round(class_balance.get(1, 0), 2)
        }
        self.report["data_quality"] = dq_stats

        self.logger.info(f"Found duplicates: {duplicates}. Missing values: {total_missing}. Class balance: {class_balance}")

        if total_missing > 0.05:
            self.logger.warning("Warning: The data contains more than 5% missing values!")
        if class_balance.get(1, 0) < 0.1:
             self.logger.warning("Warning: severe class imbalance!")

    def train_and_evaluate(self) -> None:
        self.logger.info("Data preparation and splitting (Train/Test)...")
        X = self.df.drop('default', axis=1)
        y = self.df['default']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

        self.logger.info(f"Model initialization {self.model_name}...")
        model_params = {
            'learning_rate': 0.05,
            'max_depth': 5,
            'n_estimators': 200,
            'subsample': 0.8,
            'eval_metric': 'logloss',
            'random_state': 42
        }
        model = XGBClassifier(**model_params)
        self.report["model_name"] = self.model_name
        self.report["model_parameters"] = model_params

        self.logger.info("Cross-validation (consistency of results)...")
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='f1')
        self.report["cross_validation_f1_mean"] = round(cv_scores.mean(), 4)
        self.logger.info(f"CV F1-score: {cv_scores.mean():.4f}")

        self.logger.info("Model training...")
        model.fit(X_train, y_train)

        self.logger.info("Evaluation on a test sample...")
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "Accuracy": round(accuracy_score(y_test, y_pred), 4),
            "Precision": round(precision_score(y_test, y_pred), 4),
            "Recall": round(recall_score(y_test, y_pred), 4),
            "F1-score": round(f1_score(y_test, y_pred), 4),
            "ROC-AUC": round(roc_auc_score(y_test, y_prob), 4)
        }
        self.report["metrics"] = metrics
        
        if metrics["Recall"] > 0.80 and metrics["ROC-AUC"] > 0.85:
            self.report["business_conclusion"] = "The model is suitable for use. The risk of missing defaults is minimized."
        else:
            self.report["business_conclusion"] = "The model needs further refinement. The recall is too low."

        self.logger.info(f"Metrics: {metrics}")
        self.model = model

    def save_artifacts(self) -> None:
        self.logger.info("Saving results...")
        
        # Create a unique directory for the current run
        os.makedirs(self.run_dir, exist_ok=True)

        # Save the artifacts inside this folder
        joblib.dump(self.model, self.model_save_path)
        
        with open(self.report_save_path, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, indent=4, ensure_ascii=False)

        self.logger.info(f"All artifacts have been saved to a folder: {self.run_dir}")
        self.logger.info("The ML-conveyor has successfully completed its operation!")

    def run(self) -> None:
        self.load_data()
        self.check_data_quality()
        self.train_and_evaluate()
        self.save_artifacts()

if __name__ == "__main__":
    config = load_config()
    DATA = config["data"]["cleaned_data"]
    ARTIFACTS_DIR = config["directories"]["artifacts_dir"]
    MODEL_NAME = config["ml"]["model_name"]

    pipeline = CreditScoringPipeline(DATA, ARTIFACTS_DIR, model_name=MODEL_NAME)
    pipeline.run()