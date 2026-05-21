import pytest
import pandas as pd
import numpy as np
import joblib
import logging
from config.config import load_config

# --- Fixtures (Preparing data for tests) ---
@pytest.fixture(scope="session")
def logger():
    """Test logger fixture"""
    test_logger = logging.getLogger("PyTest_ML")
    test_logger.setLevel(logging.INFO)
    # To prevent pytest logs from being duplicated when we configure them
    if not test_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)
    return test_logger

@pytest.fixture
def config():
    return load_config()

@pytest.fixture
def model(config, logger):
    MODEL_PATH = config["ml"]["working_model_path"]
    logger.info(f"Loading model for tests from: {MODEL_PATH}")
    return joblib.load(MODEL_PATH)

@pytest.fixture
def sample_valid_data(config, logger):
    DATA_PATH = config["data"]["cleaned_data"]
    logger.info(f"Loading sample data for tests from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    X = df.drop(columns=['default', 'client_id'], errors='ignore')
    return X.head(5)

# --- Test scenarios ---
def test_prediction_output_types(model, sample_valid_data, logger):
    """Scenario 1: Checking the correctness of the types of input values"""
    logger.info("Starting Scenario 1: Output types validation...")
    
    predictions = model.predict(sample_valid_data)
    probabilities = model.predict_proba(sample_valid_data)
    
    assert isinstance(predictions, np.ndarray), "Forecasts must be in the form of a NumPy array"
    assert isinstance(probabilities, np.ndarray), "The probabilities must be in the form of a NumPy array"
    assert predictions.dtype in [np.int32, np.int64], "Predicted classes must be integers"
    
    logger.info("Scenario 1 Passed: Output types are correct.")

def test_probability_boundaries(model, sample_valid_data, logger):
    """Scenario 2: Checking boundary conditions (probability boundaries)"""
    logger.info("Starting Scenario 2: Probability boundaries validation...")
    
    probabilities = model.predict_proba(sample_valid_data)[:, 1]
    
    assert np.all(probabilities >= 0.0), "A probability of less than 0.0 has been found!"
    assert np.all(probabilities <= 1.0), "A probability greater than 1.0 has been found!"
    
    logger.info("Scenario 2 Passed: Probabilities are strictly between 0.0 and 1.0.")

def test_empty_data_handling(model, logger):
    """Scenario 3: Handling empty or invalid data"""
    logger.info("Starting Scenario 3: Empty data handling validation...")
    
    empty_df = pd.DataFrame()
    
    with pytest.raises(ValueError):
        model.predict(empty_df)
        
    logger.info("Scenario 3 Passed: Model successfully raised ValueError on empty data.")