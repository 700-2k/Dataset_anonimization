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
    return dicts.districts[coords]


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


def get_k_anonymity(table: pd.DataFrame, quasi_ids: list[str]) -> int:
    grouped = table.groupby(quasi_ids).size()
    k = grouped.min()
    return k


# ============================================
# 🧪 Testing
# ============================================


def get_anonymized_columns(table: pd.DataFrame, quasi_ids: list[str]) -> tuple:
    unique_columns_table = table[quasi_ids].drop_duplicates()
    count_unique_columns = len(unique_columns_table)
    return (unique_columns_table, count_unique_columns)


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

    quasi_ids = ["store_name", "coordinates", "date-time"]

    filtred_table, count_unique = get_anonymized_columns(table, quasi_ids)

    export_output(filtred_table, out_path)

    print(get_k_anonymity(table, quasi_ids))
