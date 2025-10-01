import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, date
import json
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

UNAS_API_BASE = os.getenv('UNAS_API_BASE')
UNAS_API_KEY = os.getenv("UNAS_API_KEY")

SESSION_TIMEOUT = 20  # másodperc

def data_dir_with_filename(fname: str) -> str:
    return f"data/{fname}"

def _xml(params_dict: dict) -> str:
    """<Params> body builder"""
    root = ET.Element("Params")
    for k, v in params_dict.items():
        e = ET.SubElement(root, k)
        e.text = str(v)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")

def txt(el: ET.Element, path: str) -> str:
    f = el.find(path)

    return f.text.strip() if f is not None and f.text is not None else ""


def unas_login(api_key: str) -> str:
    url = f"{UNAS_API_BASE}/login"
    body = _xml({"ApiKey": api_key, "WebshopInfo": "true"})
    headers = {"Content-Type": "application/xml"}
    resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()
    tree = ET.fromstring(resp.text)
    token_el = tree.find("Token")

    if token_el is None or not token_el.text:
        raise RuntimeError(f"Nem sikerült tokent kinyerni. Válasz: {resp.text[:500]}")

    return token_el.text.strip()

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

def write_response_xml_file(string: str, fname: str) -> None:
    os.makedirs("data", exist_ok=True)

    with open(data_dir_with_filename(fname), "w", encoding="utf-8") as f:
        f.write(string)

ALLOWED_CUSTOMER_GROUPS = {"", "Alapértelmezett", "SAP9-Törzsvásárló"}

# Item names containing these substrings will be SKIPPED (case-insensitive)
SKIP_ITEM_NAME_SUBSTRINGS = (
    "szállítási költség",
    "utánvét kezelési költség",
)

# Order columns to duplicate on every item row
ORDER_COLUMNS = {
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

def _flatten_element(elem: ET.Element, base_path: str = "") -> dict:
    """
    Recursively flatten an XML element into {path: value}.
    - Leaf nodes -> 'base/Tag': text
    - Attributes -> 'base/@attr': value
    - Repeated children are indexed: base/Options/Option[1]/Name
    """
    out = {}

    # attributes
    if elem.attrib:
        for k, v in elem.attrib.items():
            out[(base_path + "/@" + k).strip("/")] = v

    children = list(elem)
    if not children:
        out[base_path.strip("/")] = (elem.text or "").strip()

        return out

    # group by tag to detect repeats
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
    """Return True if the item's Name contains any forbidden substring."""
    name = txt(item_elem, "Name").lower()
    return any(substr in name for substr in SKIP_ITEM_NAME_SUBSTRINGS)

def xml_to_excel_one_sheet(xml_path: str, out_xlsx: str | None = None) -> str:
    """
    Create ONE Excel sheet 'OrderItems_ALL':
      - one row per <Order>/<Items>/<Item>
      - duplicates Order_* fields on every row
      - includes ALL Item_* leaf fields (recursively flattened)
      - SKIPS items whose Name contains any forbidden substring
    Filter: only orders with Customer/Group/Name in ALLOWED_CUSTOMER_GROUPS.
    """
    xml_path = str(xml_path)

    if out_xlsx is None:
        out_xlsx = f"{xml_path.split('.')[0]}.xlsx"

    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows = []

    for o in root.findall(".//Order"):
        group_name = txt(o, "Customer/Group/Name")

        if group_name not in ALLOWED_CUSTOMER_GROUPS:
            continue  # skip this order entirely

        # Build the order context once (duplicated to each item row)
        order_ctx = {}
        for col_name, xpath in ORDER_COLUMNS.items():
            order_ctx[col_name] = txt(o, xpath)

        # For each item, create a row with duplicated order fields + all item fields
        for idx, it in enumerate(o.findall("./Items/Item"), start=1):

            # --- SKIP unwanted item names ---
            if _should_skip_item_by_name(it):
                continue

            flat_item = _flatten_element(it, "Item")  # ALL item leaves + attributes

            # Ensure some common item fields exist (nice to have)
            for k in ["Item/Id", "Item/Sku", "Item/Name", "Item/Quantity", "Item/Unit",
                      "Item/PriceNet", "Item/PriceGross", "Item/Vat", "Item/Status"]:
                flat_item.setdefault(k, "")

            # Prefix item keys to become valid column names
            item_dict = {("Item_" + k.replace("/", "_")).replace("[", "_").replace("]", ""): v
                         for k, v in flat_item.items()}

            # add line number (within order)
            item_dict["LineNo"] = idx

            # merge order context + item dict
            row = {**order_ctx, **item_dict}
            rows.append(row)

    # If no rows, still write a headered sheet
    if not rows:
        base_cols = list(ORDER_COLUMNS.keys()) + ["LineNo", "Item_Id", "Item_Sku", "Item_Name"]
        df_empty = pd.DataFrame(columns=base_cols)

        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xlw:
            df_empty.to_excel(xlw, sheet_name="OrderItems_ALL", index=False)

        return out_xlsx

    # Finalize columns: put Order_* first, then LineNo, then common Item_* fields, then everything else (sorted)
    all_cols = set().union(*[set(r.keys()) for r in rows])
    front_order = list(ORDER_COLUMNS.keys()) + ["LineNo",
        "Item_Id", "Item_Sku", "Item_Name", "Item_Quantity", "Item_Unit",
        "Item_PriceNet", "Item_PriceGross", "Item_Vat", "Item_Status"
    ]

    front = [c for c in front_order if c in all_cols]
    rest = sorted([c for c in all_cols if c not in front])

    df = pd.DataFrame(rows, columns=front + rest)

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as xlw:
        df.to_excel(xlw, sheet_name="OrderItems_ALL", index=False)

    return out_xlsx

# -----------------------------
# Pull orders from UNAS
# -----------------------------
def get_all_orders(date_start: str, date_end: str) -> str:
    orders_ = unas_call("getOrder", {"DateStart": date_start, "DateEnd": date_end})

    return ET.tostring(orders_, encoding="unicode")

def weekly_ranges_back(months=1, fmt="%Y.%m.%d") -> list:
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

# -----------------------------
# Token helpers
# -----------------------------
def set_token(token: str) -> None:
    with open("token.txt", "w", encoding="utf-8") as f:
        f.write(token)

def get_token() -> str:
    with open("token.txt", "r", encoding="utf-8") as f:
        return f.read().strip()


def main() -> None:
    # Get UNAS token
    token = unas_login(UNAS_API_KEY)
    print(f"Token megszerezve. {token}")
    set_token(token)

    # Fetch today's orders
    today = datetime.now().strftime("%Y.%m.%d")
    response: str = get_all_orders(
        date_start=today,
        date_end=today
    )

    fname = "today.xml"
    write_response_xml_file(response, fname)

    src = f"data/{fname}"
    out = xml_to_excel_one_sheet(src)
    print(f"Export kész: {out}")

if __name__ == "__main__":
    main()
