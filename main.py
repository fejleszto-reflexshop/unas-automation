import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, date
import json
import os
from dotenv import load_dotenv

load_dotenv()

# TODO: see description
"""
- szabad e az unas adatb ben valtoztatni adatokat ha nem akkor mi legyen

"""

UNAS_API_BASE = os.getenv('API_BASE')
UNAS_API_KEY = os.getenv("KEY")

SESSION_TIMEOUT = 20  # másodperc

def data_dir_with_filename(fname):
    return f"data/{fname}"

def _xml(params_dict: dict) -> str:
    """
    Egyszerű XML body gyártó az <Params> csomóhoz.
    { 'ApiKey': 'abc', 'WebshopInfo': 'true' } -> <Params><ApiKey>abc</ApiKey><WebshopInfo>true</WebshopInfo></Params>
    """
    root = ET.Element("Params")
    for k, v in params_dict.items():
        e = ET.SubElement(root, k)
        e.text = str(v)
    # Ha szükséges a fejléc:
    xml_body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_body.decode("utf-8")


def unas_login(api_key: str) -> str:
    """
    Login azonosítás API kulccsal.
    Visszaadja a Bearer tokent (string).
    """
    url = f"{UNAS_API_BASE}/login"
    body = _xml({"ApiKey": api_key, "WebshopInfo": "true"})
    headers = {
        "Content-Type": "application/xml",  # az UNAS a nyers XML-t várja
    }
    resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=SESSION_TIMEOUT)
    resp.raise_for_status()

    tree = ET.fromstring(resp.text)
    token_el = tree.find("Token")
    if token_el is None or not token_el.text:
        raise RuntimeError(f"Nem sikerült tokent kinyerni. Válasz: {resp.text[:500]}")
    return token_el.text.strip()


def unas_call(method: str, params: dict) -> ET.Element:
    """
    Általános hívó: bármely shop/* metódusra (pl. getOrder, getProduct, stb.)
    Visszaadja az XML gyökérelemet (ElementTree Element).
    """
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
    with open(data_dir_with_filename(fname), "w", encoding="utf-8") as f:
        f.write(string)


def find_and_modify_order(fname: str):
    tree = ET.parse(data_dir_with_filename(fname))
    root = tree.getroot()

    for item in root.findall("Order"):
        status = item.find("Status").text
        if status == "Nem vette át":

            new_status = "" # TODO: get newest status inside other xml files
            order_key = item.find("Key").text

            unas_call(
                method="setOrder",
                params={
                    "Action": "modify",
                    "Key": order_key,
                    "Status": new_status,
                    "StatusDateMod": datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
                }
            )


def get_all_orders(date_start: str, date_end: str) -> str:
    orders_ = unas_call(
        method="getOrder",
        params={
            "DateStart": date_start,
            "DateEnd": date_end,
        }
    )

    return ET.tostring(orders_, encoding="unicode")


def weekly_ranges_back(months=1, fmt="%Y.%m.%d"):
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


def save_week_ranges():
    json.dump(weekly_ranges_back(), open("weekly_ranges.json", "w", encoding="utf-8"))


def get_week_ranges():
    data = json.load(open("weekly_ranges.json", encoding="utf-8"))

    weeks = {}

    for line in data:
        weeks[line["weeks_ago"]] = f"{line['start']}-{line['end']}"

    return weeks


def check_order_status():
    """
    eloszor megkell nezni a heteket get_week_ranges(),
    utana be kell olvasni az elso xml fajlt es venni az elso ordert es vegig menni a tobbi xml fajlban es le checkolni a status
    - ha van valtozas a statusban akkor azt az ordert felulirni a statust a legkozelebbit kell
    - nem vette at -> torolve
    - ha torolve akkor azt hagyjuk
    egyenlore ennyi majd kesobb tajekoztatasra van szukseg
    :return: None
    """

    for file in os.listdir("data"):
        if file.endswith(".xml"):
            find_and_modify_order(file)


def set_token(token: str):
    with open("token.txt", "w", encoding="utf-8") as f:
        f.write(token)

def get_token():
    with open("token.txt", "r", encoding="utf-8") as f:
        return f.read().strip()

def main():
    token = unas_login(UNAS_API_KEY)
    print(f"Token megszerezve. {token}")
    set_token(token)

    # today = str(datetime.now().strftime("%Y.%m.%d"))

    # orders_from_today = get_all_orders(date_start=today, date_end=today)
    # write_response_xml_file(orders_from_today, "orders_from_today.xml")

    # for week in weekly_ranges_back(months=1, fmt="%Y.%m.%d"):
    #     write_response_xml_file(
    #         get_all_orders(
    #             week['start'],
    #             week['end']
    #         ),
    #         f"week_{week['start']}-{week['end']}.xml"
    #     )

    print(f"Orders megszerezve.")


if __name__ == "__main__":
    main()
    # check_order_status()
