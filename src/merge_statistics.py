"""
Merge all webshop into one stats
and also make for every webshop unique stats
"""
import os
import pandas as pd
from datetime import datetime


def data_dir_with_file(fname: str) -> str:
    return f"../data/{fname}"

def reflexshop_stats() -> None:
    today = datetime.now().strftime("%Y.%m.%d")
    df_today = pd.read_excel(data_dir_with_file(f"today_unas_{today}.xlsx"))

    orders_today = len(df_today.values)

    print("count order: ", orders_today)

    filtered = filter(lambda row: str(row) != 'nan', df_today['Order_Paid'])

    print("count filtered: ", len(list(filtered)))

    for i in filtered:
        print(i)

def popfanatic_stats() -> None:
    pass

def merge_stats() -> None:
    pass

def main():
    reflexshop_stats()
    popfanatic_stats()
    merge_stats()

if __name__ == "__main__":
    main()
