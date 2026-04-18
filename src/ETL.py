import pandas as pd
import os

def extract_data(file_path: str) -> pd.DataFrame:
    """
    Extract data from Excel file.

    :param file_path: path to raw data file
    :return: pandas DataFrame
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_excel(file_path)

    return df