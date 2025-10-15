import os
from datetime import datetime, date, timedelta
from openpyxl import load_workbook
from dotenv import load_dotenv
from openpyxl.workbook import Workbook
import pandas as pd

load_dotenv()
download_folder = os.getenv("DOWNLOAD_DIR")

# -------------------------
# Helpers
# -------------------------
def rename_excel_sheet(wb: Workbook, new_sheet_name: str, path: str) -> None:
    sheet = wb[wb.sheetnames[0]]
    sheet.title = new_sheet_name
    wb.save(path)

def rename_excel(excel_path: str, to: str) -> None:
    os.rename(excel_path, to)

def filter_excel(excel_path: str) -> None:
    """
    Keep only required columns and ensure 'Termék egységára' exists.
    - Renames various 'nettó ár' variants to 'Termék egységára'.
    - If no direct unit-price column exists, computes it from 'Nettó Összesen' / 'Mennyiség' when available.
    """
    import unicodedata
    import pandas as pd
    import numpy as np

    def norm(s: str) -> str:
        if s is None:
            return ""
        s = unicodedata.normalize("NFKC", str(s))
        s = s.replace("\u00A0", " ")  # non-breaking space -> normal space
        return s.strip().lower()

    # columns we ultimately want to keep (with final display names)
    desired_cols = [
        "Rendelés szám", "Vásárló csoport", "E-mail", "Dátum",
        "Szállítási Mód", "Fizetési Mód", "Megrendelés státusz",
        "Száll. Város", "Száll. Ir.", "Száll. Ország", "Adószám",
        "Szállítási Díj", "Kezelési Költség", "Kedvezmény",
        "Termék Név", "Cikkszám", "Mennyiség", "Termék egységára",  # <- final name here
        "Nettó Összesen"  # keep if you use it in summaries
    ]

    # possible aliases for the unit net price in source files
    unit_price_aliases = {
        "nettó ár", "netto ár", "netto ar", "nettó ar",
        "nettó egységár", "egységár (nettó)", "egysegar (netto)",
        "unit net price", "net unit price", "unit price (net)"
    }

    df = pd.read_excel(excel_path)

    # Map normalized header -> original header
    norm_to_orig = {norm(c): c for c in df.columns}

    # 1) Find a unit net price column
    unit_col_orig = None
    for alias in unit_price_aliases:
        if alias in norm_to_orig:
            unit_col_orig = norm_to_orig[alias]
            break

    # 2) If found: rename it to 'Termék egységára'
    if unit_col_orig:
        if unit_col_orig != "Termék egységára":
            df.rename(columns={unit_col_orig: "Termék egységára"}, inplace=True)
    else:
        # 3) If not found: try to compute from Nettó Összesen / Mennyiség
        sum_col = norm_to_orig.get("nettó összesen") or norm_to_orig.get("netto osszesen") or norm_to_orig.get("netto összesen")
        qty_col = norm_to_orig.get("mennyiség") or norm_to_orig.get("mennyiseg")
        if sum_col and qty_col:
            with np.errstate(all="ignore"):
                df["Termék egységára"] = pd.to_numeric(df[sum_col], errors="coerce") / pd.to_numeric(df[qty_col], errors="coerce")
        else:
            # If neither source nor computable, at least create empty column so it's present
            df["Termék egységára"] = np.nan

    # 4) Keep only desired columns that actually exist (avoid KeyError)
    keep = [c for c in desired_cols if c in df.columns]
    df = df[keep]

    # 🔹 Replace empty 'Vásárló csoport' with '|'
    vc = "Vásárló csoport"
    if vc in df.columns:
        mask = df[vc].isna() | (df[vc].astype(str).str.strip() == "")
        df.loc[mask, vc] = "|"

    df['Bolt neve'] = 'Reflexshop'

    # 5) Save back
    df.to_excel(excel_path, index=False)


def summarize_orders_into_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    rows = len(df)

    wb = load_workbook(path)
    day_name = wb.active.title  # should be the same as file base (YYYY-MM-DD)

    cols_net = ["Nettó Összesen", "Kedvezmény"]
    cols_gross = ["Szállítási Díj", "Kezelési Költség"]
    vat_multiplier = 0.73

    net_sum = df[cols_net].sum().sum() if all(col in df.columns for col in cols_net) else 0
    gross_sum = (df[cols_gross].sum().sum() * vat_multiplier) if all(col in df.columns for col in cols_gross) else 0
    total_revenue = net_sum + gross_sum

    out = pd.DataFrame({
        day_name: [rows, total_revenue],
        "": ["", ""]
    }, index=["Orders", "Revenue"])

    return out

def merge_all_daily_summaries(folder_path: str) -> None:
    files = sorted(
        [f for f in os.listdir(folder_path) if f.lower().endswith(".xlsx") and f.startswith("day-")]
    )
    if not files:
        return

    all_data = pd.DataFrame()
    for file in files:
        file_path = os.path.join(folder_path, file)
        df_summary = summarize_orders_into_excel(file_path)
        all_data = pd.concat([all_data, df_summary], axis=1)

    out_path = os.path.join(folder_path, "daily-summary.xlsx")
    all_data.to_excel(out_path)
    print(f"✅ Saved merged summary to {out_path}")

# -------------------------
# Main workflow
# -------------------------
def move_files_into_webshop_folders() -> None:
    # 1) Move every root-level .xlsx into webshop folders
    for file in os.listdir(download_folder):
        if not file.lower().endswith(".xlsx"):
            continue

        try:
            webshop_name = file.split("_")[1].split("-")[0]
            if webshop_name == 'tesztpr':
                webshop_name = 'jatekfarm'
            if webshop_name == 'toymarket':
                webshop_name = 'tarsasjatekrendeles'
        except Exception:
            webshop_name = "unknown"

        folder_path = os.path.join(download_folder, webshop_name)
        os.makedirs(folder_path, exist_ok=True)

        src = os.path.join(download_folder, file)
        dst = os.path.join(folder_path, file)
        if os.path.abspath(src) != os.path.abspath(dst):
            os.replace(src, dst)

    # 2) Per webshop folder: rename to day-YYYY-MM-DD.xlsx (newest → today)
    today = date.today()

    for folder in os.listdir(download_folder):
        folder_path = os.path.join(download_folder, folder)
        if not os.path.isdir(folder_path):
            continue

        # Gather only raw day files to rename (exclude already-produced outputs)
        xlsx_files = [
            f for f in os.listdir(folder_path)
            if f.lower().endswith(".xlsx")
            and not f.startswith("day-")
            and not f.startswith("year-")
            and f.lower() != "daily-summary.xlsx"
        ]
        if not xlsx_files:
            continue

        xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(folder_path, f)), reverse=True)

        # Rename each file to day-YYYY-MM-DD.xlsx with decreasing dates
        for idx, fname in enumerate(xlsx_files):
            target_date = today - timedelta(days=idx-1)  # keep your original logic
            base_date = target_date.isoformat()          # YYYY-MM-DD
            new_base = f"day-{base_date}"
            new_name = f"{new_base}.xlsx"

            src = os.path.join(folder_path, fname)
            dst = os.path.join(folder_path, new_name)

            # Ensure uniqueness (if some file already has the same name)
            final_dst = dst
            if os.path.exists(final_dst):
                i = 1
                while True:
                    candidate = os.path.join(folder_path, f"{new_base}_{i}.xlsx")
                    if not os.path.exists(candidate):
                        final_dst = candidate
                        break
                    i += 1

            os.rename(src, final_dst)

            # Try to set sheet title to the bare date (YYYY-MM-DD) for summaries
            try:
                wb = load_workbook(final_dst)
                rename_excel_sheet(wb, base_date, final_dst)
            except Exception as e:
                print(f"⚠️ Sheet rename skipped for {final_dst}: {e}")

            print(f"📦 {folder}: '{fname}' → '{os.path.basename(final_dst)}'")

        # 3) After day- renaming: pick the largest day-*.xlsx and rename to year-<YYYY>.xlsx
        day_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".xlsx") and f.startswith("day-")]
        if day_files:
            largest_file = max(day_files, key=lambda f: os.path.getsize(os.path.join(folder_path, f)))
            largest_path = os.path.join(folder_path, largest_file)
            year_name = f"year-{today.year}.xlsx"
            year_path = os.path.join(folder_path, year_name)

            if os.path.abspath(largest_path) != os.path.abspath(year_path):
                if os.path.exists(year_path):
                    os.remove(year_path)
                os.rename(largest_path, year_path)

                # 🔹 FILTER the year file too
                filter_excel(year_path)

                # Ensure the year sheet name is the current year
                try:
                    wb = load_workbook(year_path)
                    rename_excel_sheet(wb, str(today.year), year_path)
                except Exception as e:
                    print(f"⚠️ Year sheet rename skipped for {year_path}: {e}")
                print(f"🏷️  {folder}: '{os.path.basename(largest_path)}' → '{year_name}'")

        # 4) Create daily-summary.xlsx from all filtered day-*.xlsx in this folder
        merge_all_daily_summaries(folder_path)

def delete_unnecessary_files(download_dir: str) -> None:
    for folder in os.listdir(download_dir):
        folder_path = os.path.join(download_dir, folder)
        if not os.path.isdir(folder_path):
            continue
        for file in os.listdir(folder_path):
            if file.startswith('day-') and file.lower().endswith(".xlsx"):
                os.remove(os.path.join(folder_path, file))

def main() -> None:
    move_files_into_webshop_folders()
    delete_unnecessary_files(download_folder)

if __name__ == "__main__":
    main()
