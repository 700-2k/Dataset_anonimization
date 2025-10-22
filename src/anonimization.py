import pandas as pd
from pathlib import Path


# ============================================
# 📦 Data loading
# ============================================


def Load_table(path: str) -> pd.DataFrame:
    path = Path(path)
    table = pd.read_excel(path)
    return table


if __name__ == "__main__":
    path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\data\\table.xlsx"
