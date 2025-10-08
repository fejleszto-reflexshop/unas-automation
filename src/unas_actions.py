from src.unas_helper import *
from src.unas_helper import _existing_week_labels_in_sheet, _get_existing_header, _open_or_init_wb_with_header
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
# -----------------------------
# Setup
# -----------------------------
load_dotenv()
UNAS_API_BASE = os.getenv('UNAS_API_BASE')

def build_monthly_workbook_for_previous_weeks(
        months_back: int,
        out_xlsx: str,
        spacer_rows: int
) -> str:
    """
    APPEND-ONLY, IDEMPOTENT:
      - hónaponként külön sheet (YYYY-MM),
      - CSAK a hiányzó hetek kerülnek letöltésre és hozzáfűzésre,
      - meglévő heti blokkok NEM törlődnek / íródnak felül,
      - ha a hét két hónapot érint, mindkét lapon megjelenik (csak ha még nincs ott).
    A táblázat rendelés-szintű (xml_string_to_dataframe) — 1 sor / rendelés.
    """
    today = date.today()
    cur_month_first = date(today.year, today.month, 1)

    start_month_year = cur_month_first.year
    start_month = cur_month_first.month - months_back
    while start_month <= 0:
        start_month += 12
        start_month_year -= 1

    window_start = date(start_month_year, start_month, 1)
    window_end = today

    weeks = weekly_ranges_between(window_start, window_end)

    # Ha létezik fájl, előre beolvassuk a már létező heti címkéket és fejlécet
    existing_wb = load_workbook(out_xlsx) if os.path.exists(out_xlsx) else None
    existing_labels_by_sheet: dict[str, set[str]] = {}
    existing_headers_by_sheet: dict[str, list[str]] = {}

    if existing_wb:
        for sheet in existing_wb.sheetnames:
            ws = existing_wb[sheet]
            existing_labels_by_sheet[sheet] = _existing_week_labels_in_sheet(ws)
            existing_headers_by_sheet[sheet] = _get_existing_header(ws)

    def sheet_has_label(sheet_name: str, label: str) -> bool:
        return label in existing_labels_by_sheet.get(sheet_name, set())

    # Menet közben mindig csak a hiányzó heteket kérjük le
    for (ws_start, ws_end) in weeks:
        start_s = ws_start.strftime("%Y.%m.%d")
        end_s = ws_end.strftime("%Y.%m.%d")
        label = f"{start_s}-{end_s}"

        months_hit = [month_sheet_name(m) for m in week_months_covered(ws_start, ws_end)]

        # Ha minden érintett lapon megvan már, ugorjuk
        if months_hit and all(sheet_has_label(sheet, label) for sheet in months_hit):
            continue

        # Most kérjük le csak ezt a hiányzó hetet
        xml_week = get_all_orders(date_start=start_s, date_end=end_s)
        df_week = xml_string_to_dataframe(xml_week)
        if df_week.empty:
            continue

        # Írjuk ki minden olyan lapra, ahol még hiányzik
        for sheet in months_hit:
            # Fejléc: ha lap létezik, megőrizzük a meglévőt, különben ORDER_COLUMNS kulcsokkal indítunk
            header_cols = existing_headers_by_sheet.get(sheet)
            if not header_cols or all(h is None for h in header_cols):
                header_cols = list(ORDER_COLUMNS.keys())

            # Igazítsuk a df-et a fejléc oszlopaihoz
            df_aligned = df_week.reindex(columns=header_cols, fill_value="")

            wb, ws = _open_or_init_wb_with_header(out_xlsx, sheet, header_cols)

            # Frissítsük a cache-t, ha új lap jött létre
            if sheet not in existing_labels_by_sheet:
                existing_labels_by_sheet[sheet] = _existing_week_labels_in_sheet(ws)
                existing_headers_by_sheet[sheet] = _get_existing_header(ws)

            if label not in existing_labels_by_sheet[sheet]:
                append_week_block(ws, df_aligned, label=label, spacer_rows=spacer_rows)
                wb.save(out_xlsx)
                existing_labels_by_sheet[sheet].add(label)

    print(f"Monthly workbook ready (append-only): {out_xlsx}")
    return out_xlsx


# -----------------------------
# Fetchers (file-per-run exports)
# -----------------------------
def fetch_today_orders_and_export_excel(out_path: str, day_else: Optional[str] = None) -> str:
    day = day_else or datetime.now().strftime("%Y.%m.%d")
    response = get_all_orders(date_start=day, date_end=day)
    fname_xml = f"today.xml"
    write_response_xml_file(response, fname_xml)

    src_xml = f"../data/{fname_xml}"
    out_xlsx = out_path
    export_xml_file_to_excel_one_sheet(src_xml, out_xlsx, sheet_name="Mai nap")
    print(f"Export kész: {out_xlsx}")

    return out_xlsx


def fetch_previous_months_orders_and_export_excel(shop_name: str) -> None:
    """(Opcionális régi funkció) Heti fájlok külön xml-be."""
    save_week_ranges()

    for week in get_week_ranges().values():
        start_date, end_date = week.split('-')
        fname_xml = f"../data/{shop_name}_week_{start_date}-{end_date}.xml"
        write_response_xml_file(get_all_orders(start_date, end_date), fname_xml)

        print("Export xml ready:", fname_xml)


# -----------------------------
# Daily job
# -----------------------------
def daily_summary_orders_to_excel(output_path: str = "../data/orders_unas_main.xlsx",
                                  sheet_name: str = "Napokra bontva",
                                  spacer_rows: int = 3) -> None:
    today_str = datetime.now().strftime("%Y.%m.%d")
    yday_str = (date.today() - timedelta(days=1)).strftime("%Y.%m.%d")
    xml_today = get_all_orders(date_start=today_str, date_end=today_str)
    xml_yday = get_all_orders(date_start=yday_str, date_end=yday_str)

    write_response_xml_file(xml_today, "today.xml")
    write_response_xml_file(xml_yday, "yesterday.xml")

    df_today = xml_string_to_dataframe(xml_today)
    df_yday = xml_string_to_dataframe(xml_yday)
    header_cols = list(df_today.columns) if len(df_today.columns) >= len(df_yday.columns) else list(df_yday.columns)
    _open_or_init_wb_with_header(output_path, sheet_name, header_cols)[0].save(output_path)
    deleted = delete_batch_by_label(output_path, sheet_name, yday_str, header_cols)

    if deleted:
        print(f"• Removed previous partial day for {yday_str}")

    prepend_batch_to_excel(df_yday, output_path, batch_label=yday_str, sheet_name=sheet_name, spacer_rows=spacer_rows)
    prepend_batch_to_excel(df_today, output_path, batch_label=today_str, sheet_name=sheet_name, spacer_rows=spacer_rows)

    print(f"✔ Rotated batches. Top = TODAY({today_str}), below = YESTERDAY(full {yday_str}). File: {output_path}")


def combine_excel_files_with_day_and_daily_data(today_file_path: str,
                                                daily_summary_file_path: str,
                                                output_path: str) -> None:
    df1 = pd.read_excel(today_file_path)
    df2 = pd.read_excel(daily_summary_file_path)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Ma", index=False)
        df2.to_excel(writer, sheet_name="Napok", index=False)

    print(f"Combined {today_file_path} and {daily_summary_file_path} into {output_path}.")


# ----------------------------------------------------------------
# Core OOP building blocks
# ----------------------------------------------------------------

@dataclass(frozen=True)
class ShopConfig:
    name: str
    env_key: str                   # ENV var holding the API key
    folder_slug: str               # folder under ../data/, e.g. "reflexshop"
    months_back: int = 3
    spacer_rows: int = 5
    base_dir: Path = Path("../data")

class ShopBase:
    """
    Template Method base class:
      - handles auth
      - builds consistent output paths
      - runs the three loaders and final combine
    Subclasses only set config (env key + folder slug + nice name).
    """

    def __init__(self, config: ShopConfig) -> None:
        self.config = config
        self._api_key: Optional[str] = os.getenv(config.env_key)
        self.folder = (config.base_dir / config.folder_slug)
        self.folder.mkdir(parents=True, exist_ok=True)
        self.is_save_workbook_into_excel: bool = False
        self.workbook_json_path: str = ''
        self.emails_seen: dict[str, int] = {}

        stem = config.folder_slug
        self.today_out_path = str(self.folder / f"{stem}_today.xlsx")
        self.daily_out_path = str(self.folder / f"{stem}_daily_summary_orders.xlsx")
        self.prev_months_out_path = str(self.folder / f"{stem}_workbook.xlsx")
        self.combined_out_path = str(self.folder / f"{stem}_combined.xlsx")

    # ---- Hooks you can override in subclasses if you ever need custom behavior ----
    def authenticate(self) -> None:
        if not self._api_key:
            raise RuntimeError(f"[{self.config.name}] Missing API key in env: {self.config.env_key}")

        unas_token(self._api_key)

    def load_today_workbook(self) -> None:
        fetch_today_orders_and_export_excel(out_path=self.today_out_path)

    def load_daily_summary_workbook(self) -> None:
        daily_summary_orders_to_excel(output_path=self.daily_out_path)

    def load_prev_months_workbook(self) -> None:
        build_monthly_workbook_for_previous_weeks(
            months_back=self.config.months_back,
            out_xlsx=self.prev_months_out_path,
            spacer_rows=self.config.spacer_rows,
        )

    def combine_outputs(self) -> None:
        combine_excel_files_with_day_and_daily_data(
            today_file_path=self.today_out_path,
            daily_summary_file_path=self.daily_out_path,
            output_path=self.combined_out_path,
        )

    def save_workbook_into_json(self) -> None:
        """ call xml_to_json() """
        if self.is_save_workbook_into_excel:
            raise NotImplementedError


    def xml_to_json(self) -> None:
        pass

    def count_emails(self) -> None:
        """ call save_workbook_into_json() """

        with open(self.workbook_json_path, "r") as file:
            json_data = json.load(file, encoding="utf-8")

        for item in json_data:
            if item["email"] not in self.emails_seen:
                self.emails_seen[item["email"]] = 1
            else:
                self.emails_seen[item["email"]] += 1

    # ---- Template method ----
    def main(self) -> None:
        self.authenticate()

        self.load_today_workbook()
        self.load_daily_summary_workbook()
        self.load_prev_months_workbook()

        self.combine_outputs()

        self.count_emails()


class Reflexshop(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Reflexshop",
            env_key="UNAS_REFLEXSHOP_API_KEY",
            folder_slug="reflexshop",
        ))

class Okostojasjatek(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Okostojasjatek",
            env_key="UNAS_OKOSTOJASJATEK_API_KEY",
            folder_slug="okostojasjatek",
        ))

class Ordoglakatok(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Ordoglakatok",
            env_key="UNAS_ORDOGLAKATOK_API_KEY",
            folder_slug="ordoglakatok",
        ))

class Tarsas(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Tarsas",
            env_key="UNAS_TARSAS_API_KEY",
            folder_slug="tarsas",
        ))

class Tarsasjatekdiszkont(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Tarsasjatekdiszkont",
            env_key="UNAS_TARSASJATEKDISZKONT_API_KEY",
            folder_slug="tarsasjatekdiszkont",
        ))

class Tarsasjatekvasar(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Tarsasjatekvasar",
            env_key="UNAS_TARSASJATEKVASAR_API_KEY",
            folder_slug="tarsasjatekvasar",
        ))

class Jatekfarm(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Jatekfarm",
            env_key="UNAS_JATEKFARM_API_KEY",
            folder_slug="jatekfarm",
        ))

class Tarsasjatekrendeles(ShopBase):
    def __init__(self) -> None:
        super().__init__(ShopConfig(
            name="Tarsasjatekrendeles",
            env_key="UNAS_TARSASJATEKRENDELES_API_KEY",
            folder_slug="tarsasjatekrendeles",
        ))


def run_all_shops(exclude_shop: list[str]) -> None:
    shops: dict[str, ShopBase] = {
        "reflexshop": Reflexshop(),
        "okostojasjatek": Okostojasjatek(),
        "ordoglakatok": Ordoglakatok(),
        "tarsas": Tarsas(),
        "tarsasjatekdiszkont": Tarsasjatekdiszkont(),
        "tarsasjatekvasar": Tarsasjatekvasar(),
        "jatekfarm": Jatekfarm(),
        "tarsasjatekrendeles": Tarsasjatekrendeles(),
    }

    for shop in shops.keys():
        if shop not in exclude_shop:
            print(f"==> Running {shops[shop].config.name}")
            shops[shop].main()
            print(f"\tDone: {shops[shop].combined_out_path}")
        else:
            print(f"==> Skipping {shops[shop].config.name}")

if __name__ == "__main__":
    # run a single shop:
    # Reflexshop().main()

    # exclude shop's e.g. exclude reflexshop
    run_all_shops(exclude_shop=['reflexshop'])

    # or run all:
    # run_all_shops(exclude_shop=[''])


