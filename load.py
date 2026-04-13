import os
import pandas as pd

def load_to_csv(db: pd.DataFrame, output_path: str):
    # Load
    # Save cleaned and transformed dataset
    db.to_csv(output_path, index=False)