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
# ✅ Data validation and rules
# ============================================


def table_validate(table: pd.DataFrame) -> pd.DataFrame:
    table["cards_number"] = table["cards_number"].astype(str)
    return table


def correct_output(table: pd.DataFrame) -> pd.DataFrame:
    table = table.rename(
        {
            "store_name": "store",
            "date-time": "year-month",
            "coordinates": "district",
        }
    )
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
    card = str(card)[0:4] + "************"
    return card


def anonymize_date_time(date_time: str) -> datetime:
    date_time = datetime.fromisoformat(date_time)
    start_hour = (date_time.hour // 8) * 8
    end_hour = start_hour + 8

    return f"{date_time:%Y-%m}"
    # return f"{date_time.date()}"  # T{start_hour:02d}:00-{end_hour:02d}:00"


def anonymize_store(store: str) -> str:
    store = dicts.anonymized_stores[store]
    return store


def anonymize_coords(coords: str) -> str:
    return dicts.districts[coords]


def anonymize_total_cost(cost: int) -> str:
    match cost:
        case n if 0 <= n <= 500:
            return "<=500"
        case n if 500 < n <= 1000:
            return "500-1000"
        case n if 1000 < n <= 2000:
            return "1000-2000"
        case n if 2000 < n <= 5000:
            return "2000-5000"
        case n if 5000 < n <= 10000:
            return "5000-10000"
        case n if 10000 < n <= 30000:
            return "10000-30000"
        case n if 30000 < n <= 50000:
            return "30000-50000"
        case n if 50000 < n <= 100000:
            return "50000-100000"
        case n if n > 100000:
            return "100000+"


def anonymize_num_products(num: int) -> str:
    match num:
        case 1:
            return "1"
        case n if 2 <= n <= 3:
            return "2-3"
        case n if 4 <= n <= 6:
            return "4-6"
        case n if n > 6:
            return "6+"


def anonymize_price(price: int) -> str:
    match price:
        case n if 0 <= n <= 500:
            return "<=500"
        case n if 500 < n <= 1000:
            return "500-1000"
        case n if 1000 < n <= 2000:
            return "1000-2000"
        case n if 2000 < n <= 5000:
            return "2000-5000"
        case n if 5000 < n <= 10000:
            return "5000-10000"
        case n if 10000 < n <= 30000:
            return "10000-30000"
        case n if 30000 < n <= 50000:
            return "30000-50000"
        case n if 50000 < n <= 100000:
            return "50000-100000"
        case n if n > 100000:
            return "100000+"


def anonymize_categories(cat: str) -> str:
    cat = dicts.categories[cat]
    return cat


def anonymize_brand(brand: str) -> str:
    brand = dicts.brands[brand]
    return brand


methods = {
    "cards_number": anonymize_card_number,
    "date-time": anonymize_date_time,
    "store_name": anonymize_store,
    "coordinates": anonymize_coords,
    "total_cost": anonymize_total_cost,
    "number_of_products": anonymize_num_products,
    "price": anonymize_price,
    "categories": anonymize_categories,
    "brands": anonymize_brand,
}


# ============================================
# ⚙️ Data Processing
# ============================================


def get_good_k(table: pd.DataFrame) -> int:
    rows = len(table)

    match rows:
        case n if n <= 51000:
            return 10
        case n if n <= 105000:
            return 7
        case n if n <= 260000:
            return 5


def anonymize_column(table: pd.DataFrame, column: str) -> pd.DataFrame:
    table[column] = table[column].apply(methods[column])
    return table


def anonymize_direct_identifiers(table: pd.DataFrame) -> pd.DataFrame:
    table = anonymize_column(table, "cards_number")

    table = table.drop("receipt_id", axis=1)
    return table


def get_k_anonymity(table: pd.DataFrame, quasi_ids: list[str]) -> tuple:
    # count k
    grouped = table.groupby(quasi_ids).size().rename("group_size").reset_index()

    # lowest good k-anonymity
    k = get_good_k(table)

    # add sizes of groups to table
    table_with_sizes = table.merge(grouped, on=quasi_ids, how="left")

    # count % of good k group
    number_good = (table_with_sizes["group_size"] >= k).sum()
    total = len(table)
    fraction_good = number_good / total * 100

    # count percent of lowest k groups
    bad_group_list = []
    for bad_k in range(1, k):
        total_bad = len(table_with_sizes[table_with_sizes["group_size"] == bad_k])
        fraction_bad = total_bad / total * 100
        if fraction_bad == 0:
            continue
        bad_group_list.append([bad_k, f"{fraction_bad:.2f}%"])
        if len(bad_group_list) == 5:
            break

    return (f"{fraction_good:.2f}%", f"{k}", bad_group_list)


def full_anonymization(table: pd.DataFrame) -> pd.DataFrame:
    table = anonymize_direct_identifiers(table)

    table = anonymize_column(table, "date-time")

    table = anonymize_column(table, "store_name")

    table = anonymize_column(table, "coordinates")

    table = anonymize_column(table, "total_cost")

    table = anonymize_column(table, "number_of_products")

    table = anonymize_column(table, "price")

    table = anonymize_column(table, "categories")

    table = anonymize_column(table, "brands")

    return table


# ============================================
# 🚹 User Interface
# ============================================


def print_result(fraction: str, k_anonymity: int, bad_k: list):
    print(f"Приемлемое значение K-Anonymity для данного датасета: k = {k_anonymity}")
    print(f"Процент строк с k >= {k_anonymity}: {fraction}")
    if len(bad_k) > 0:
        print("Плохие K-Anonymity и их процент в датасете:")
        print("  K-Anonymity  |  Процент  ")
        for k in bad_k:
            print(f"       {k[0]}       |   {k[1]}   ")
    else:
        print("Нет плохих K-Anonymity")


def get_quasis(keys: str) -> list:
    key_dict = {
        "1": "store_name",
        "2": "date-time",
        "3": "coordinates",
        "4": "categories",
        "5": "brands",
        "6": "price",
        "7": "cards_number",
        "8": "number_of_products",
        "9": "total_cost",
    }
    keys = keys.split()
    result = []
    for key in keys:
        result.append(key_dict[key])
    return result


def user_interface(table: pd.DataFrame):
    print("Укажите квази-идентификаторы по их номеру через пробел:")
    print(
        "1: store_name\n"
        "2: date-time\n"
        "3: coordinates\n"
        "4: categories\n"
        "5: brands\n"
        "6: price\n"
        "7: cards_number\n"
        "8: number_of_products\n"
        "9: total_cost\n"
    )
    keys = input()
    quasi_ids = get_quasis(keys)

    fraction, k_anonymity, bad_k = get_k_anonymity(table, quasi_ids)

    print_result(fraction, k_anonymity, bad_k)

    print("Количесвто уникальных по заданным квази-идентификаторам:")

    count = table.groupby(quasi_ids).ngroups
    print(count)


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
    data_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\data\\table1.xlsx"
    out_path = "C:\\Users\\VLAD\\prog\\Dataset_anonimization\\output\\example.xlsx"

    table = Load_table(data_path)

    table = table_validate(table)

    table = full_anonymization(table)

    export_output(table, out_path)

    user_interface(table)
