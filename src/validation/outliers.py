import pandas as pd

def zscore_outliers(df: pd.DataFrame, col: str, threshold: float = 3.0) -> pd.DataFrame:
    if col not in df.columns or df[col].std() == 0:
        return df.iloc[0:0]  # empty
    z = (df[col] - df[col].mean()) / df[col].std()
    return df[abs(z) > threshold]