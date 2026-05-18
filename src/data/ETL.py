import pandas as pd
import sqlite3
import os

def validate_raw_data(df: pd.DataFrame, logger=None) -> None:
    required_columns = [
        'ID', 'LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE', 'AGE',
        'PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6',
        'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
        'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6',
        'default payment next month'
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        err_msg = f"Missing required columns: {missing_columns}"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    if df.empty:
        err_msg = "Dataset is empty"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    if df['default payment next month'].isnull().any():
        err_msg = "Target column contains missing values"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    valid_target_values = {0, 1}
    actual_target_values = set(df['default payment next month'].unique())
    if not actual_target_values.issubset(valid_target_values):
        err_msg = f"Target column contains invalid values: {actual_target_values}"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)
    
    if logger: logger.info("Raw data validation passed successfully")


def validate_transformed_data(df: pd.DataFrame, logger=None) -> None:
    required_columns = [
        'limit_bal', 'sex', 'education', 'marriage', 'age',
        'pay_0', 'pay_2', 'pay_3', 'pay_4', 'pay_5', 'pay_6',
        'bill_amt1', 'bill_amt2', 'bill_amt3', 'bill_amt4', 'bill_amt5', 'bill_amt6',
        'pay_amt1', 'pay_amt2', 'pay_amt3', 'pay_amt4', 'pay_amt5', 'pay_amt6',
        'default', 'avg_delay', 'max_delay', 'total_bill', 'total_paid', 'payment_ratio'
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        err_msg = f"Missing transformed columns: {missing_columns}"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    if 'id' in df.columns:
        err_msg = "Column 'id' should have been removed during transformation"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    if df['default'].isnull().any():
        err_msg = "Column 'default' contains missing values"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)

    derived_cols = ['avg_delay', 'max_delay', 'total_bill', 'total_paid', 'payment_ratio']
    if df[derived_cols].isnull().any().any():
        err_msg = "Derived columns contain missing values"
        if logger: logger.error(err_msg)
        raise ValueError(err_msg)
    
    if logger: logger.info("Transformed data validation passed")


def extract_data(file_path: str, logger=None) -> pd.DataFrame:
    if logger: logger.info(f"Extracting data from: {file_path}")

    if not os.path.exists(file_path):
        err_msg = f"File not found: {file_path}"
        if logger: logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    df = pd.read_excel(file_path)
    if logger: logger.info(f"Loaded {len(df)} rows from Excel")

    validate_raw_data(df, logger=logger)

    return df


def transform_data(df: pd.DataFrame, logger=None) -> pd.DataFrame:
    df = df.copy()
    initial_count = len(df)
    if logger: logger.info("Starting data transformation")

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(".", "_", regex=False)
        .str.replace(" ", "_", regex=False)
        .str.lower()
    )

    # Rename target column
    df.rename(columns={
        "default_payment_next_month": "default"
    }, inplace=True)

    # Remove duplicates
    df.drop_duplicates(inplace=True)
    dup_count = initial_count - len(df)
    if logger and dup_count > 0:
        logger.warning(f"Removed {dup_count} duplicate rows")

    # Handle missing values
    before_dropna = len(df)
    df.dropna(subset=["id", "limit_bal", "age", "default"], inplace=True)
    nan_count = before_dropna - len(df)
    if logger and nan_count > 0:
        logger.warning(f"Dropped {nan_count} rows due to critical missing values (NaN)")

    categorical_cols = ["sex", "education", "marriage"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])

    numeric_cols = [
        "limit_bal", "age",
        "bill_amt1", "bill_amt2", "bill_amt3", "bill_amt4", "bill_amt5", "bill_amt6",
        "pay_amt1", "pay_amt2", "pay_amt3", "pay_amt4", "pay_amt5", "pay_amt6"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # Convert data types
    int_cols = [
        "id", "limit_bal", "sex", "education", "marriage", "age",
        "pay_0", "pay_2", "pay_3", "pay_4", "pay_5", "pay_6",
        "bill_amt1", "bill_amt2", "bill_amt3", "bill_amt4", "bill_amt5", "bill_amt6",
        "pay_amt1", "pay_amt2", "pay_amt3", "pay_amt4", "pay_amt5", "pay_amt6",
        "default"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype(int)

    # Filter irrelevant / incorrect records
    df = df[df["age"] > 0]
    df = df[df["limit_bal"] > 0]

    # Drop ID because it is unnecessary for model
    if "id" in df.columns:
        df.drop(columns=["id"], inplace=True)

    # Fix category values
    df["education"] = df["education"].replace([0, 5, 6], 4)
    df["marriage"] = df["marriage"].replace(0, 3)

    # Create new features
    pay_cols = ["pay_0", "pay_2", "pay_3", "pay_4", "pay_5", "pay_6"]
    df["avg_delay"] = df[pay_cols].mean(axis=1)
    df["max_delay"] = df[pay_cols].max(axis=1)

    bill_cols = ["bill_amt1", "bill_amt2", "bill_amt3", "bill_amt4", "bill_amt5", "bill_amt6"]
    df["total_bill"] = df[bill_cols].sum(axis=1)

    pay_amt_cols = ["pay_amt1", "pay_amt2", "pay_amt3", "pay_amt4", "pay_amt5", "pay_amt6"]
    df["total_paid"] = df[pay_amt_cols].sum(axis=1)

    df["payment_ratio"] = df["total_paid"] / (df["total_bill"] + 1)

    validate_transformed_data(df, logger=logger)

    if logger: 
        logger.info(f"Transformation complete. Final row count: {len(df)}")

    return df


def load_data(df: pd.DataFrame, db_path: str, table_name: str, logger=None) -> None:
    if logger: logger.info(f"Loading data into database at {db_path}...")
    conn = sqlite3.connect(db_path)

    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        msg = f"Data loaded successfully into table '{table_name}' ({len(df)} rows)"
        if logger: logger.info(msg)
    except Exception as e:
        if logger: logger.error(f"Failed to load data to database: {e}")
        raise
    finally:
        conn.close()