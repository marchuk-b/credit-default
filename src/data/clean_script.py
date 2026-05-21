import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

def clean_and_standardize(df_input: pd.DataFrame, logger=None):
    df_cleaned = df_input.copy()
    initial_len = len(df_cleaned)
    
    if logger: logger.info("Step 3.1: Removing duplicates...")
    df_cleaned = df_cleaned.drop_duplicates()
    
    if logger: logger.info("Step 3.2: Applying Isolation Forest to detect anomalies...")
    numeric_cols = df_cleaned.select_dtypes(include=['number']).columns
    
    clf = IsolationForest(contamination=0.01, random_state=42)
    df_cleaned['anomaly'] = clf.fit_predict(df_cleaned[numeric_cols])
    
    df_cleaned = df_cleaned[df_cleaned['anomaly'] == 1]
    df_cleaned = df_cleaned.drop(columns=['anomaly'])
    
    if logger: logger.info("Step 3.3: Scaling numerical features with StandardScaler...")
    scaler = StandardScaler()
    cols_to_scale = ['limit_bal', 'age', 'total_bill', 'total_paid']
    
    df_cleaned[cols_to_scale] = scaler.fit_transform(df_cleaned[cols_to_scale])
    if logger: 
        logger.info(f"Cleaning completed. Deleted {initial_len - len(df_cleaned)} rows (duplicates + anomalies).")
    else:
        print(f"Cleaning completed. Deleted {initial_len - len(df_cleaned)} rows (duplicates + anomalies).")
    
    return df_cleaned, scaler
