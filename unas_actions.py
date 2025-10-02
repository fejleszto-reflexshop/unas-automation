import os
import json
from datetime import datetime, timedelta, date
from typing import Dict, Optional
import requests
import xml.etree.ElementTree as ET
from copy import deepcopy
from dotenv import load_dotenv
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
UNAS_API_BASE = os.getenv('UNAS_API_BASE')
UNAS_API_KEY = os.getenv("UNAS_API_KEY")
SESSION_TIMEOUT = 20  # sec

def data_dir_with_filename(fname: str) -> str:
    os.makedirs("data", exist_ok=True)
    return f"data/{fname}"

# -----------------------------
# Business rules
# -----------------------------
ALLOWED_CUSTOMER_GROUPS = {"", "Alapértelmezett", "SAP9-Törzsvásárló"}

SKIP_ITEM_NAME_SUBSTRINGS = (
    "szállítási költség",
    "utánvét kezelési költség",
)

ORDER_COLUMNS: Dict[str, str] = {
    "Order_Id":              "Id",
    "Order_Key":             "Key",
    "Order_Date":            "Date",
    "Order_DateMod":         "DateMod",
    "Order_Status":          "Status",
    "Order_StatusID":        "StatusID",
    "Order_Currency":        "Currency",
    "Order_SumPriceGross":   "SumPriceGross",
    "Order_Referer":         "Referer",
    "Order_CustomerEmail":   "Customer/Email",
    "Order_CustomerName":    "Customer/Contact/Name",
    "Order_CustomerLang":    "Customer/Contact/Lang",
    "Order_CustomerGroup":   "Customer/Group/Name",
    "Order_InvoiceZIP":      "Customer/Addresses/Invoice/ZIP",
    "Order_InvoiceCity":     "Customer/Addresses/Invoice/City",
    "Order_InvoiceCountry":  "Customer/Addresses/Invoice/Country",
    "Order_ShippingZIP":     "Customer/Addresses/Shipping/ZIP",
    "Order_ShippingCity":    "Customer/Addresses/Shipping/City",
    "Order_ShippingCountry": "Customer/Addresses/Shipping/Country",
    "Order_PaymentName":     "Payment/Name",
    "Order_PaymentType":     "Payment/Type",
    "Order_PaymentStatus":   "Payment/Status",
    "Order_Paid":            "Payment/Paid",
    "Order_Unpaid":          "Payment/Unpaid",
    "Order_UTM_Source":      "UTM/Source",
    "Order_UTM_Medium":      "UTM/Medium",
    "Order_UTM_Campaign":    "UTM/Campaign",
    "Order_UTM_Content":     "UTM/Content",
}

# -----------------------------
# XML helpers
# -----------------------------
def _xml(params_dict: dict) -> str:
    root = ET.Element("Params")
    for k, v in params_dict.items():
        e = ET.SubElement(root, k)
        e.text = str(v)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")

def txt(el: ET.Element, path: str) -> str:
    f = el.find(path)
    return f.text.strip() if f is not None and f.text is not None else ""

def write_response_xml_file(string: str, fname: str) -> None:
    with open(data_dir_with_filename(fname), "w", encoding="utf-8") as f:
        f.write(string)

# -----------------------------
# UNAS auth + calls
# -----------------------------
def unas_login(api_key: str) -> str:
    url = f"{UNAS_API_BASE}/login"
    body = _xml({"ApiKey": api_key, "WebshopInfo": "true"})
    headers = {"Content-Type": "application/xml"}
    resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()
    tree = ET.fromstring(resp.text)
    token_el = tree.find("Token")
    if token_el is None or not token_el.text:
        raise RuntimeError(f"Token not found. Response: {resp.text[:500]}")
    return token_el.text.strip()

def set_token(token: str) -> None:
    with open("token.txt", "w", encoding="utf-8") as f:
        f.write(token)

def get_token() -> str:
    with open("token.txt", "r", encoding="utf-8") as f:
        return f.read().strip()

def get_unas_token() -> None:
    token = unas_login(UNAS_API_KEY)
    print(f"Token OK: {token[:8]}...")  # shortened
    set_token(token)

def unas_call(method: str, params: dict) -> ET.Element:
    url = f"{UNAS_API_BASE}/{method}"
    body = _xml(params)
    headers = {
        "Content-Type": "application/xml",
        "Authorization": f"Bearer {get_token()}",
    }
    resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()
    return ET.fromstring(resp.text)

def get_all_orders(date_start: str, date_end: str) -> str:
    orders_elem = unas_call("getOrder", {"DateStart": date_start, "DateEnd": date_end})
    return ET.tostring(orders_elem, encoding="unicode")

# -----------------------------
# Flatten helpers
# -----------------------------
def _flatten_element(elem: ET.Element, base_path: str = "") -> dict:
    out = {}
    if elem.attrib:
        for k, v in elem.attrib.items():
            out[(base_path + "/@" + k).strip("/")] = v
    children = list(elem)
    if not children:
        out[base_path.strip("/")] = (elem.text or "").strip()
        return out
    by_tag = {}
    for child in children:
        by_tag.setdefault(child.tag, []).append(child)
    for tag, nodes in by_tag.items():
        if len(nodes) == 1:
            child_path = f"{base_path}/{tag}".strip("/")
            out.update(_flatten_element(nodes[0], child_path))
        else:
            for idx, node in enumerate(nodes, start=1):
                child_path = f"{base_path}/{tag}[{idx}]".strip("/")
                out.update(_flatten_element(node, child_path))
    return out

def _should_skip_item_by_name(item_elem: ET.Element) -> bool:
    name = txt(item_elem, "Name").lower()
    return any(substr in name for substr in SKIP_ITEM_NAME_SUBSTRINGS)

# -----------------------------
# Optional: Combine XMLs -> single <Orders>
# -----------------------------
def combine_orders_xml_strings(*xml_strings: str) -> str:
    combined = ET.Element("Orders")
    for x in xml_strings:
        if not x or not x.strip():
            continue
        root = ET.fromstring(x)
        for order in root.findall(".//Order"):
            combined.append(deepcopy(order))
    return ET.tostring(combined, encoding="utf-8", xml_declaration=True).decode("utf-8")

# -----------------------------
# XML -> DataFrame utilities
# -----------------------------
def xml_string_to_dataframe(xml_text: str) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    rows = []
    for o in root.findall(".//Order"):
        group_name = txt(o, "Customer/Group/Name")
        if group_name not in ALLOWED_CUSTOMER_GROUPS:
            continue
        order_ctx = {col_name: txt(o, xpath) for col_name, xpath in ORDER_COLUMNS.items()}
        for idx, it in enumerate(o.findall("./Items/Item"), start=1):
            if _should_skip_item_by_name(it):
                continue
            flat_item = _flatten_element(it, "Item")
            for k in ["Item/Id", "Item/Sku", "Item/Name", "Item/Quantity", "Item/Unit",
                      "Item/PriceNet", "Item/PriceGross", "Item/Vat", "Item/Status"]:
                flat_item.setdefault(k, "")
            item_dict = {("Item_" + k.replace("/", "_")).replace("[", "_").replace("]", ""): v
                         for k, v in flat_item.items()}
            item_dict["LineNo"] = idx
            rows.append({**order_ctx, **item_dict})
    if not rows:
        base_cols = list(ORDER_COLUMNS.keys()) + ["LineNo",
            "Item_Id", "Item_Sku", "Item_Name", "Item_Quantity", "Item_Unit",
            "Item_PriceNet", "Item_PriceGross", "Item_Vat", "Item_Status"
        ]
        return pd.DataFrame(columns=base_cols)
    all_cols = set().union(*[set(r.keys()) for r in rows])
    front_order = list(ORDER_COLUMNS.keys()) + ["LineNo",
        "Item_Id", "Item_Sku", "Item_Name", "Item_Quantity", "Item_Unit",
        "Item_PriceNet", "Item_PriceGross", "Item_Vat", "Item_Status"
    ]
    front = [c for c in front_order if c in all_cols]
    rest = sorted([c for c in all_cols if c not in front])
    return pd.DataFrame(rows, columns=front + rest)

def xml_file_to_dataframe(xml_path: str) -> pd.DataFrame:
    """Read an XML file with <Orders>/<Order>... and return the same tabular DataFrame as xml_string_to_dataframe."""
    xml_text = open(xml_path, "r", encoding="utf-8").read()
    return xml_string_to_dataframe(xml_text)

# -----------------------------
# DataFrame -> new Excel utilities
# -----------------------------
def write_dataframe_to_new_excel(df: pd.DataFrame, out_xlsx: str, sheet_name: str = "OrderItems_ALL") -> str:
    """Write df into a NEW Excel file (overwrites if exists), with headers. Uses openpyxl for consistency with append/prepend flows."""
    os.makedirs(os.path.dirname(out_xlsx) or ".", exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xlw:
        df.to_excel(xlw, sheet_name=sheet_name, index=False)
    return out_xlsx

def export_xml_file_to_excel_one_sheet(xml_path: str, out_xlsx: Optional[str] = None, sheet_name: str = "OrderItems_ALL") -> str:
    """Convenience wrapper: XML file -> DataFrame -> new Excel file."""
    if out_xlsx is None:
        out_xlsx = f"{os.path.splitext(xml_path)[0]}.xlsx"
    df = xml_file_to_dataframe(xml_path)
    return write_dataframe_to_new_excel(df, out_xlsx, sheet_name=sheet_name)

# --- Compatibility shim (so old code continues to work) ---
def xml_to_excel_one_sheet(xml_path: str, out_xlsx: Optional[str] = None) -> str:
    return export_xml_file_to_excel_one_sheet(xml_path, out_xlsx, sheet_name="OrderItems_ALL")

# -----------------------------
# Helpers to manage batches in Excel
# -----------------------------
def _open_or_init_wb_with_header(xlsx_path: str, sheet_name: str, header_cols: list):
    """Open workbook and ensure sheet exists with header in row 1."""
    if os.path.exists(xlsx_path):
        wb = load_workbook(xlsx_path)
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            if ws.max_row < 1:
                ws.append(header_cols)
        else:
            ws = wb.create_sheet(title=sheet_name)
            ws.append(header_cols)
    else:
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name
        ws.append(header_cols)
    return wb, ws

def _find_batch_bounds(ws, label: str) -> Optional[tuple]:
    """Find the row range (start, end) for a batch labeled exactly 'Batch: <label>' in column A."""
    target = f"Batch: {label}"
    start = None
    for r in range(1, ws.max_row + 1):
        val = ws.cell(row=r, column=1).value
        if isinstance(val, str) and val == target:
            start = r
            break
    if start is None:
        return None
    next_batch = None
    for r in range(start + 1, ws.max_row + 1):
        val = ws.cell(row=r, column=1).value
        if isinstance(val, str) and val.startswith("Batch: "):
            next_batch = r
            break
    end = (next_batch - 1) if next_batch else ws.max_row
    return start, end

def delete_batch_by_label(xlsx_path: str, sheet_name: str, label: str, header_cols: list) -> bool:
    """Delete existing batch block (if present). Returns True if deleted."""
    wb, ws = _open_or_init_wb_with_header(xlsx_path, sheet_name, header_cols)
    bounds = _find_batch_bounds(ws, label)
    if not bounds:
        wb.save(xlsx_path)
        return False
    start, end = bounds
    amount = end - start + 1
    ws.delete_rows(start, amount)
    wb.save(xlsx_path)
    return True

def prepend_batch_to_excel(df: pd.DataFrame, xlsx_path: str, batch_label: str, sheet_name: str = "OrderItems_ALL", spacer_rows: int = 3) -> None:
    """Insert at TOP (below header): label + DF rows + per-day summary + spacer rows."""
    header_cols = list(df.columns)
    wb, ws = _open_or_init_wb_with_header(xlsx_path, sheet_name, header_cols)
    n_rows = len(df)
    total_to_insert = 1 + n_rows + 1 + spacer_rows  # label + data + summary + spacer
    ws.insert_rows(idx=2, amount=total_to_insert)
    ws.cell(row=2, column=1, value=f"Batch: {batch_label}")
    start_data_row = 3
    for i, row_vals in enumerate(dataframe_to_rows(df, index=False, header=False)):
        for col_idx, val in enumerate(row_vals, start=1):
            ws.cell(row=start_data_row + i, column=col_idx, value=val)
    if "Order_Key" in df.columns:
        orders_count = int(df["Order_Key"].nunique())
    elif "Order_Id" in df.columns:
        orders_count = int(df["Order_Id"].nunique())
    else:
        orders_count = int(len(df))
    summary_row_idx = start_data_row + n_rows
    ws.cell(row=summary_row_idx, column=1, value="Orders on day:")
    ws.cell(row=summary_row_idx, column=2, value=orders_count)
    wb.save(xlsx_path)

# -----------------------------
# Fetchers (file-per-run exports)
# -----------------------------
def fetch_today_orders_and_export_excel(day_else: Optional[str] = None) -> str:
    day = day_else or datetime.now().strftime("%Y.%m.%d")
    response = get_all_orders(date_start=day, date_end=day)
    fname_xml = f"today.xml"

    write_response_xml_file(response, fname_xml)

    src_xml = f"data/{fname_xml}"
    out_xlsx = f"data/today_{day}.xlsx"
    export_xml_file_to_excel_one_sheet(src_xml, out_xlsx)
    print(f"Export kész: {out_xlsx}")
    return out_xlsx

def weekly_ranges_back(months=3, fmt="%Y.%m.%d") -> list:
    today = date.today()
    this_week_monday = today - timedelta(days=today.weekday())
    prev_monday = this_week_monday - timedelta(days=7)
    prev_sunday = this_week_monday - timedelta(days=1)
    max_weeks = months * 4 + 2
    ranges = []
    weeks_ago = 1
    while weeks_ago <= max_weeks:
        ranges.append({
            "weeks_ago": weeks_ago,
            "start": prev_monday.strftime(fmt),
            "end": prev_sunday.strftime(fmt),
        })
        prev_monday -= timedelta(days=7)
        prev_sunday -= timedelta(days=7)
        weeks_ago += 1
    return ranges

def save_week_ranges() -> None:
    os.makedirs("data", exist_ok=True)
    json.dump(weekly_ranges_back(), open("weekly_ranges.json", "w", encoding="utf-8"))

def get_week_ranges() -> dict:
    data = json.load(open("weekly_ranges.json", encoding="utf-8"))
    weeks = {}
    for line in data:
        weeks[line["weeks_ago"]] = f"{line['start']}-{line['end']}"
    return weeks

def fetch_previous_months_orders_and_export_excel() -> None:
    """Fetch previous ~3 months (rolling weeks), one Excel per week."""
    save_week_ranges()
    for week in get_week_ranges().values():
        start_date, end_date = week.split('-')
        fname_xml = f"week_{start_date}-{end_date}.xml"
        write_response_xml_file(get_all_orders(start_date, end_date), fname_xml)
        src_xml = f"data/{fname_xml}"
        out_xlsx = f"data/week_{start_date}-{end_date}.xlsx"
        export_xml_file_to_excel_one_sheet(src_xml, out_xlsx)
        print("Export ready:", out_xlsx, src_xml)

def fetch_between_given_dates_orders_and_export_excel(start_date: str, end_date: str) -> str:
    fname_xml = f"between_{start_date}-{end_date}.xml"
    write_response_xml_file(get_all_orders(start_date, end_date), fname_xml)
    src_xml = f"data/{fname_xml}"
    out_xlsx = f"data/between_{start_date}-{end_date}.xlsx"
    export_xml_file_to_excel_one_sheet(src_xml, out_xlsx)
    print("Export ready:", out_xlsx, src_xml)
    return out_xlsx

def fetch_today_and_yesterday_orders_and_export_excel() -> str:
    today_str = datetime.now().strftime("%Y.%m.%d")
    yday_str  = (date.today() - timedelta(days=1)).strftime("%Y.%m.%d")
    xml_today = get_all_orders(date_start=today_str, date_end=today_str)
    xml_yday  = get_all_orders(date_start=yday_str,  date_end=yday_str)
    combined_xml = combine_orders_xml_strings(xml_today, xml_yday)
    fname_xml = "days.xml"
    write_response_xml_file(combined_xml, fname_xml)
    src_xml = f"data/{fname_xml}"
    out_xlsx = f"data/days.xlsx"
    export_xml_file_to_excel_one_sheet(src_xml, out_xlsx)
    print("Export ready:", out_xlsx, src_xml)
    return out_xlsx

# -----------------------------
# Daily job (18:00): replace yesterday's "today-partial" with full-yesterday, then add today's partial
# -----------------------------
def daily_summary_orders_to_excel(output_path: str = "data/orders_main.xlsx", sheet_name: str = "OrderItems_ALL", spacer_rows: int = 3) -> None:
    """Get today and yesterday orders and append into a single summary workbook with top-insert + per-day summary."""

    today_str = datetime.now().strftime("%Y.%m.%d")
    yday_str  = (date.today() - timedelta(days=1)).strftime("%Y.%m.%d")
    xml_today = get_all_orders(date_start=today_str, date_end=today_str)
    xml_yday  = get_all_orders(date_start=yday_str,  date_end=yday_str)

    write_response_xml_file(xml_today, "today.xml")
    write_response_xml_file(xml_yday, "yesterday.xml")

    df_today = xml_string_to_dataframe(xml_today)
    df_yday  = xml_string_to_dataframe(xml_yday)
    header_cols = list(df_today.columns) if len(df_today.columns) >= len(df_yday.columns) else list(df_yday.columns)

    _open_or_init_wb_with_header(output_path, sheet_name, header_cols)[0].save(output_path)
    deleted = delete_batch_by_label(output_path, sheet_name, yday_str, header_cols)

    if deleted:
        print(f"• Removed previous partial batch for {yday_str}")

    prepend_batch_to_excel(df_yday,  output_path, batch_label=yday_str,  sheet_name=sheet_name, spacer_rows=spacer_rows)
    prepend_batch_to_excel(df_today, output_path, batch_label=today_str, sheet_name=sheet_name, spacer_rows=spacer_rows)

    print(f"✔ Rotated batches. Top = TODAY({today_str}), below = YESTERDAY(full {yday_str}). File: {output_path}")

# -----------------------------
# Main
# -----------------------------
def main() -> None:
    get_unas_token()

    # === Choose what you want to run ===
    fetch_today_orders_and_export_excel()
    fetch_today_and_yesterday_orders_and_export_excel()
    # fetch_between_given_dates_orders_and_export_excel("2025.09.25", "2025.10.02")
    # fetch_previous_months_orders_and_export_excel()

    daily_summary_orders_to_excel()  # writes to data/orders_main.xlsx

if __name__ == "__main__":
    main()
