# Credit Default Prediction System (MLOps Pipeline)

A fully automated, end-to-end Machine Learning pipeline designed to predict credit card defaults. This project features a decoupled architecture separating the heavy offline model training (including data generation and anomaly detection) from a lightweight, real-time production inference conveyor.

## Key Features

* **Decoupled MLOps Architecture:** Separate pipelines for model training and real-time inference.
* **Automated ETL:** Robust data extraction, validation, and feature engineering.
* **Synthetic Data Generation:** Utilizes **CTGAN** to balance minority classes in historical data.
* **Automated Data Quality:** Uses **Isolation Forest** for anomaly detection and dynamically saves/loads a `StandardScaler`.
* **Real-time Production Watcher:** A `watchdog`-powered conveyor that automatically detects new client files, scales data, and generates risk predictions.
* **Robust Logging & Monitoring:** Centralized file/console logging and automated Cross-Validation monitoring.

## Tech Stack
- **Language:** Python 3.13+
- **Machine Learning:** XGBoost, Scikit-learn, CTGAN
- **Data Processing:** Pandas, NumPy
- **Database:** SQLite3
- **Orchestration:** Watchdog
- **Testing:** Pytest

## Project Structure
```text
credit-default/
│
├── artifacts/             # Stored models (model.pkl) and scalers (scaler.pkl)
├── config/
│   ├── config.py          # Configuration loader
│   └── configs.yaml       # Centralized paths and hyperparameters
├── data/
│   ├── raw/               # Raw historical data and incoming files for watcher
│   ├── processed/         # Cleaned datasets and temp files
│   └── archive/           # Processed client files moved here by the watcher
├── logs/                  # System logs (training, inference, conveyor)
├── src/
│   ├── data/              # ETL, CTGAN, and Data Cleaning scripts
│   ├── models/            # ML Training pipeline and Inference service
│   ├── orchestration/     # The primary pipelines (Training & Watcher)
│   └── utils/             # Global utilities (e.g., centralized logger)
├── tests/                 # Pytest unit tests and CV monitor
└── requirements.txt       # Project dependencies
```

## Installation & Setup
1. Clone the repository:
```
git clone https://github.com/marchuk-b/credit-default.git
cd credit-default
```

2. Create a virtual environment:
```
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. Install dependencies:
```
pip install -r requirements.txt
```

4. Configure paths:
Ensure the directories specified in `config/configs.yaml` exist, or the system will generate them automatically upon first run.

## How to Run
1. Training Pipeline (Model Creation)
Run this pipeline when you have a large batch of new historical data. It will perform ETL, run CTGAN to balance classes, detect anomalies, train the XGBoost model, and save both `model.pkl` and `scaler.pkl` to the `artifacts/` directory.
    ```
    python -m src.orchestration.training_pipeline
    ```
    Note: By default, the pipeline uses data from the `test data` folder. If you want to use custom data, replace the `data.xls` file in the test data folder with your own file (keeping the exact same name). Alternatively, open `training_pipeline.py` and modify the path directly:
    ```
    raw_path = config["data"]["test_data"]
    ```

2. Inference Pipeline (Real-Time Watcher)
Run this script to start the 24/7 background listener. It watches the `input_dir` (defined in `configs.yaml`). Whenever a new `.csv` or `.xlsx` file containing new client data is dropped into the folder, the pipeline will instantly:
    - Perform basic transformations.
    - Scale the data using the saved `scaler.pkl` (preventing data leakage).
    - Predict default risk (`High/Medium/Low Risk`) and save the results to SQLite and a time-stamped CSV.
    - Move the original file to the `archive` directory.
    ```
    python -m src.orchestration.watcher_conveyor
    ```

## Testing & Monitoring
**Run Unit Tests:**
Validates data types, prediction boundaries, and edge cases (like empty files) using Pytest.
```
pytest tests/test_ml_pipeline.py -v
```

**Run Cross-Validation Monitor:**
Evaluates the stability of the currently deployed model across different data folds to detect potential overfitting.
```
python -m tests.cv_monitor
```

## Developer
**Bohdan Marchuk**
- Email: marchukbohdan29@gmail.com
- GitHub: [github.com/marchuk-b](https://github.com/marchuk-b)