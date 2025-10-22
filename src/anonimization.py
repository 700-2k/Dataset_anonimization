import pandas as pd
from pathlib import Path
from datetime import datetime


import dictionaries as dicts


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


def anonymize_date_time(date_time: str) -> datetime:
    date_time = datetime.fromisoformat(date_time)
    start_hour = (date_time.hour // 4) * 4
    end_hour = start_hour + 4

    return f"{date_time.date()}T{start_hour:02d}:00-{end_hour:02d}:00"


def anonymize_store(store: str) -> str:
    store = dicts.anonymized_stores[store]
    return store


def anonymize_coords(coords: str) -> str:
    coords = coords.split(",")
    lon = f"{float(coords[0]):.2f}"
    lat = f"{float(coords[1]):.2f}"
    rounded_coords = lon + "," + lat
    return rounded_coords


methods = {
    "cards_number": anonymize_card_number,
    "date-time": anonymize_date_time,
    "store_name": anonymize_store,
    "coordinates": anonymize_coords,
}


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


# ============================================
# ▶️ Main
# ============================================


if __name__ == "__main__":
    data_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\data\\table.xlsx"
    out_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\output\\example.xlsx"

    table = Load_table(data_path)

    table = table_validate(table)

    table = anonymize_direct_identifiers(table)

    table = anonymize_column(table, "date-time")

    table = anonymize_column(table, "store_name")

    table = anonymize_column(table, "coordinates")

    export_output(table, out_path)
