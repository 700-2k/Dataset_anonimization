import pandas as pd
from pathlib import Path


# ============================================
# 📦 Data loading
# ============================================


def Load_table(path: str) -> pd.DataFrame:
    path = Path(path)
    table = pd.read_excel(path)
    return table


# ============================================
# ✅ Data validation
# ============================================


def table_validate(table: pd.DataFrame) -> pd.DataFrame:
    table["cards_number"] = table["cards_number"].astype(str)
    return table


# ============================================
# 💾 Data export
# ============================================


def export_output(table: pd.DataFrame, path: str):
    path = Path(path)
    table.to_excel(path, index=False)


# ============================================
# 🔒 Anonimization functions
# ============================================


def anonymize_card_number(card: str) -> str:
    card = "************" + str(card)[12:16]
    return card


methods = {"cards_number": anonymize_card_number}


# ============================================
# ⚙️ Data Processing
# ============================================


def anonymize_column(table: pd.DataFrame, column: str) -> pd.DataFrame:
    table[column] = table[column].apply(methods[column])
    return table


def anonymize_direct_identifiers(table: pd.DataFrame) -> pd.DataFrame:
    table = anonymize_column(table, "cards_number")
    table = table.drop("receipt_id", axis=1)
    return table


if __name__ == "__main__":
    data_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\data\\table.xlsx"
    out_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\output\\example.xlsx"

    table = Load_table(data_path)

    table = table_validate(table)

    table = anonymize_direct_identifiers(table)

    export_output(table, out_path)
