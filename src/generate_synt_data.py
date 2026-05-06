import pandas as pd
import os
from typing import Tuple
from ctgan import CTGAN
from config.config import load_config


def generate_balanced_credit_data(train_df: pd.DataFrame, epochs=50, batch_size=500, logger=None) -> Tuple[pd.DataFrame, CTGAN]:
    config = load_config()
    target_col = config["data"]["target"]

    # Minority Class (Default = 1)
    # Default=1 is the minority
    minority_data = train_df[train_df[target_col] == 1]
    
    # These columns are categorical and encoded
    discrete_cols = [
        'sex',       # X2: 1=male, 2=female
        'education', # X3: 1=grad school, 2=university, 3=high school, 4=others
        'marriage',  # X4: 1=married, 2=single, 3=others
        'pay_0', 'pay_2', 'pay_3', 'pay_4', 'pay_5', 'pay_6', # X6-X11: status of delay
        'default'    # target value (0 or 1)
    ]
    
    msg = f"Training CTGAN for {epochs} epochs..."
    if logger:
        logger.info(msg)
    else:
        print(msg)

    syn = CTGAN(epochs=epochs, batch_size=batch_size)
    syn.fit(minority_data, discrete_columns=discrete_cols)
    
    # Calculate how many samples we need to reach a 50/50 balance
    majority_count = (train_df[target_col] == 0).sum()
    minority_count = (train_df[target_col] == 1).sum()
    n_to_generate = majority_count - minority_count
    
    msg = f"Generating {n_to_generate} synthetic default records..."
    if logger:
        logger.info(msg)
    else:
        print(msg)

    synthetic_samples = syn.sample(n_to_generate)
    
    # Save ONLY the synthetic records
    output_path = os.path.join(config["directories"]["data_proc_dir"], 'syn_data_gen_Marchuk.csv')
    synthetic_samples.to_csv(output_path, index=False)
    
    msg = f"Synthetic data saved to: {output_path}"
    if logger:
        logger.info(msg)
    else:
        print(msg)
    
    # Combine for internal use
    balanced = pd.concat([train_df, synthetic_samples], axis=0).reset_index(drop=True)
    
    return balanced, syn