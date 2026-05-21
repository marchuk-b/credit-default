import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import joblib
import os
from sklearn.model_selection import cross_val_score, StratifiedKFold
from config.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/cv_monitor.log", encoding='utf-8') 
    ]
)
logger = logging.getLogger("CV_Monitor")

logger.info("Launch of the automated monitoring and cross-validation module...")

# Load data
config = load_config()
logger.info(f"Loading data from {config['data']['cleaned_data']}...")
df = pd.read_csv(config["data"]["cleaned_data"])
X = df.drop(columns=['default', 'client_id'], errors='ignore')
y = df['default']

# Load the pre-trained model
model_path = config["ml"]["working_model_path"]
logger.info(f"Loading saved model from {model_path}...")
try:
    model = joblib.load(model_path)
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f"Critical error loading the model: {e}")
    raise

# Conducting a five-fold cross-validation
logger.info("Performing 5-fold cross-validation (this may take a minute)...")
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

# cross_val_score automatically uses the parameters of the loaded model
cv_scores = cross_val_score(model, X, y, cv=cv, scoring='f1')

mean_score = np.mean(cv_scores)
std_score = np.std(cv_scores)

logger.info("--- Model stability results ---")
logger.info(f"Average F1-score: {mean_score:.4f}")
logger.info(f"Standard deviation (Std): {std_score:.4f}")

if std_score < 0.02:
    logger.info("Conclusion: The model performs consistently across different data subsets.")
else:
    logger.warning("Conclusion: The model is unstable; there is a risk of overfitting.")

# Visualization results of testing
logger.info("Generating visualization...")
plt.figure(figsize=(8, 5))
sns.barplot(x=[f"Fold {i+1}" for i in range(len(cv_scores))], y=cv_scores, palette="viridis", hue=cv_scores, legend=False)
plt.axhline(mean_score, color='red', linestyle='--', label=f'Mean F1: {mean_score:.4f}')

plt.ylim(0.8, 1.0)
plt.title("Results of 5-fold cross-validation (XGBoost)")
plt.ylabel("F1 Score")
plt.xlabel("Stages of testing (Folds)")
plt.legend()
plt.tight_layout()
plt.show()

logger.info("CV Monitor completed successfully.")