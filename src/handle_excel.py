import os
from datetime import datetime, date, timedelta
from openpyxl import load_workbook

from dotenv import load_dotenv
from openpyxl.workbook import Workbook

import pandas as pd

load_dotenv()

def rename_excel_sheet(wb: Workbook, new_sheet_name: str, path: str) -> None:
    sheet = wb[wb.sheetnames[0]]

    sheet.title = new_sheet_name

    wb.save(path)


def rename_excel(excel_path: str, to: str) -> None:
    os.rename(excel_path, to)

# TODO: termek egysegara, bolt neve
def filter_excel(excel_path: str) -> None:
    keep_cols: list[str] = ["Rendelés szám", "Vásárló csoport", "E-mail", "Dátum",
                            "Szállítási Mód", "Fizetési Mód", "Megrendelés státusz",
                            "Száll. Város", "Száll. Ir.", "Száll. Ország", "Adószám",
                            "Szállítási Díj", "Kezelési Költség", "Kedvezmény",
                            "Termék Név", "Cikkszám", "Mennyiség"
                            ]

    df = pd.read_excel(excel_path)

    df = df[keep_cols]
    df['Bolt neve'] = 'Reflexshop'

    df.to_excel(excel_path, index=False)

def create_korrigalt_sheet(wb: Workbook, sheet_name: str, path: str) -> None:
    ws = wb.create_sheet(sheet_name)

    ws['A1'] = """=QUERY('2025-10'!A:BB;"select Col1, Col3, Col5 where (Col3 contains 'Alapértelmezett' or Col3 contains 'SAP9-Törzsvásárló')  and (Col16 contains 'Számlázva, átadva a futárnak' or Col16 contains 'Személyesen átvéve' or Col16 contains 'Részben számlázva, átadva a futárnak' or Col16 contains 'Előfizetés számlázva') or (Col52 contains 'WELCOMEPACK' and Col52 contains 'KLUBEVES' and Col52 contains 'KLUB3HONAPOS' and Col52 contains 'KLUB6HONAPOS')";1) """

def daily_stats(dir_path: str) -> None:
    with open("../start_date.txt", 'r') as file:
        start_date = file.readline().strip()
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        # Get today's date
        end_date = date.today()

        days = [
            (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end_date - start_date).days + 1)
        ]

    new_fnames: list[str] = []
    for index, file in enumerate(os.listdir(os.path.join(dir_path, "days"))):
        new_fname = f"{dir_path}/days/day-{days[index]}.xlsx"

        new_fnames.append(new_fname)

        os.rename(os.path.join(dir_path, "days",file), new_fname)

    for fn in new_fnames:
        new_sheet_name = fn.split('day-')[1].split('.')[0]
        wb = load_workbook(fn)
        rename_excel_sheet(wb, new_sheet_name, fn)


    for file in os.listdir(os.path.join(dir_path, "days")):
        print(file)
        summarize_orders_into_excel(os.path.join(dir_path, "days", file))

def year_stats(dir_path: str) -> None:
    for fn in os.listdir(dir_path):
        if fn.find('unas') != -1:
            excel_path = os.path.join(dir_path, fn)
            new_fname = f"{dir_path}/year-{date.today().year}.xlsx"
            rename_excel(excel_path=excel_path, to=new_fname)

            filter_excel(new_fname)

            wb = load_workbook(new_fname)
            rename_excel_sheet(wb=wb, new_sheet_name=date.today().year.__str__(), path=new_fname)

def summarize_orders_into_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    rows = len(df)

    wb = load_workbook(path)
    day_name = wb.active.title  # e.g. "2025-10-05"

    cols_net = ["Nettó Összesen", "Kedvezmény"]
    cols_gross = ["Szállítási Díj", "Kezelési Költség"]
    vat_multiplier = 0.73

    # safely calculate sums
    net_sum = df[cols_net].sum().sum() if all(col in df.columns for col in cols_net) else 0
    gross_sum = (df[cols_gross].sum().sum() * vat_multiplier) if all(col in df.columns for col in cols_gross) else 0
    total_revenue = net_sum + gross_sum

    # one date + blank column
    out = pd.DataFrame({
        day_name: [rows, total_revenue],
        "": ["", ""]
    }, index=["Orders", "Revenue"])

    return out


def merge_all_daily_summaries(dir_path: str):
    files = sorted([f for f in os.listdir(dir_path) if f.endswith(".xlsx") and f.startswith("day-")])
    all_data = pd.DataFrame()

    for file in files:
        file_path = os.path.join(dir_path, file)
        df_summary = summarize_orders_into_excel(file_path)
        all_data = pd.concat([all_data, df_summary], axis=1)

    out_path = os.path.join(dir_path, "daily_summary.xlsx")
    all_data.to_excel(out_path)
    print(f"✅ Saved merged summary to {out_path}")


def main() -> None:
    dir_path: str = os.getenv("DOWNLOAD_DIR")

    daily_stats(dir_path=dir_path)

    year_stats(dir_path=dir_path)

    merge_all_daily_summaries(dir_path=os.path.join(dir_path, "days"))


if __name__ == "__main__":
    main()
