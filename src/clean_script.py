from sklearn.preprocessing import StandardScaler
from config.config import load_config
import sqlite3
import pandas as pd

config = load_config()

def clean_and_standardize(df_input: pd.DataFrame, logger=None) -> pd.DataFrame:
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
    
    # Save
    df_cleaned.to_csv(f'{config["directories"]["data_proc_dir"]}/df_cleaned_Marchuk.csv', index=False)

    conn = sqlite3.connect(config["data"]["db_path"])
    df_cleaned.to_sql("cleaned_credit_data", conn, if_exists='replace', index=False)
    conn.close()
    
    msg = f"Cleaning completed. Deleted {initial_len - len(df_cleaned)} rows (duplicates + anomaly)."
    if logger:
        logger.info(msg)
    else:
        print(msg)

    return df_cleaned
