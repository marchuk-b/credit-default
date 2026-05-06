from sklearn.preprocessing import StandardScaler
from config.config import load_config
import pandas as pd

def clean_and_standardize(df_input: pd.DataFrame, logger=None) -> pd.DataFrame:
    config = load_config()
    df_cleaned = df_input.copy()
    
    # Delete duplicates
    initial_len = len(df_cleaned)
    df_cleaned = df_cleaned.drop_duplicates()
    
    # Delete anomaly found by Isolation Forest
    # (keep only those where anomaly == 1)
    if 'anomaly' in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned['anomaly'] == 1]
        df_cleaned = df_cleaned.drop(columns=['anomaly'])
    
    # StandardScaler
    scaler = StandardScaler()
    cols_to_scale = ['limit_bal', 'age', 'total_bill', 'total_paid']
    df_cleaned[cols_to_scale] = scaler.fit_transform(df_cleaned[cols_to_scale])
    
    msg = f"Cleaning completed. Deleted {initial_len - len(df_cleaned)} rows (duplicates + anomaly)."
    if logger:
        logger.info(msg)
    else:
        print(msg)

    return df_cleaned
