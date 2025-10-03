import os
import json
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd

load_dotenv()

SHOP_NAME     = (os.getenv("POPFANATIC_SHOP_NAME") or "").strip()
CLIENT_ID     = (os.getenv("POPFANATIC_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.getenv("POPFANATIC_CLIENT_SECRET") or "").strip()

TOKEN_URL = (os.getenv("POPFANATIC_TOKEN_URL") or f"").strip()
API_BASE  = (os.getenv("POPFANATIC_API_URL")  or "").strip()

def get_access_token() -> tuple:
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post(TOKEN_URL, headers=headers, json=payload, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    data = r.json()

    return data["access_token"], data.get("token_type", "Bearer")

def get_orders(access_token, token_type, page=0, limit=200, extra_params=None) -> dict:
    """
    Egy oldalnyi rendelést kér le. Alapból full=0, mert külön lekérjük a részleteket.
    Ha extra szűrő kell (pl. {"createdAt": "YYYY-MM-DD"}), add meg extra_params-ben.
    """
    params = {"page": page, "limit": limit, "full": 0}
    if extra_params:
        params.update(extra_params)

    url = f"{API_BASE}/orders"
    headers = {
        "Authorization": f"{token_type} {access_token}",
        "Accept": "application/json",
    }

    # Egyszerű retry 429/5xx esetekre
    for attempt in range(4):
        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)
            continue

        ct = r.headers.get("Content-Type", "")
        body = r.text if "application/json" not in ct else r.json()

        raise RuntimeError(f"Orders error {r.status_code}: {body}")

    raise RuntimeError("Orders error: retry limit exceeded")

def get_order_by_id(access_token, token_type, order_id) -> dict:
    url = f"{API_BASE}/orders/{order_id}"
    headers = {
        "Authorization": f"{token_type} {access_token}",
        "Accept": "application/json"
    }

    for attempt in range(4):
        r = requests.get(url, headers=headers, timeout=30)

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)
            continue

        raise RuntimeError(f"Order error {r.status_code}: {r.text}")

    raise RuntimeError("Order error: retry limit exceeded")

def extract_order_id(item: dict) -> str:
    """
    Robusztus ID-nyerés: href → utolsó path-szegmens (query/fragment nélkül),
    különben tipikus mezők.
    """
    href = item.get("href") or item.get("_links", {}).get("self", {}).get("href")

    if href:
        h = str(href).rstrip("/")
        # ha lenne query
        h = h.split("?", 1)[0].split("#", 1)[0]
        parts = h.split("/")

        if parts:
            return parts[-1]

    return str(item['id'])


def get_all_orders_write_into_json(access_token, token_type) -> None:
    # Ha időszak/napi szűrés kell, ide add:
    # extra_params = {"createdAt": "2025-10-03"}  # vagy {"updatedAt": "2025-10-03"}
    extra_params = None

    page = 0
    limit = 200
    total = 0

    out_path = "data/orders_popfantastic_full.ndjson"
    with open(out_path, "w", encoding="utf-8") as f_out:
        while True:
            page_data = get_orders(access_token, token_type, page=page, limit=limit, extra_params=extra_params)

            # Válasz formátum rugalmas kezelése
            items = page_data.get("items") or (page_data.get("response", {}) or {}).get("items", []) or []

            if not items:
                print(f"Page {page}: 0 item → done.")
                break

            for stub in items:
                order_id = extract_order_id(stub)
                order = get_order_by_id(access_token, token_type, order_id)
                f_out.write(json.dumps(order, ensure_ascii=False) + "\n")
                total += 1

            print(f"Page {page} kész — {len(items)} db. Eddig összesen: {total}")

            # Ha kevesebb jött, mint a limit → nincs több oldal
            if len(items) < limit:
                print("Utolsó oldal volt (len(items) < limit).")
                break

            page += 1

    print(f"VÉGE — {total} rendelést mentettem ide: {out_path}")


def get_today_orders_write_into_excel(access_token, token_type) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    extra_params = {"createdAt": today}

    data = get_orders(access_token, token_type, page=0, limit=200, extra_params=extra_params)

    out_path_json = fr"data/orders_popfantastic_{today}.json"
    out_path_excel = fr"data/orders_popfantastic_full_{today}.xlsx"

    if len(data['items']) > 0:
        for item in data['items']:
            id_ = extract_order_id(item)
            order = get_order_by_id(access_token, token_type, id_)

            with open(out_path_json, "a+", encoding="utf-8") as f_out:
                json.dump(order, f_out, ensure_ascii=False)

        df = pd.read_json(out_path_json)

        df.to_excel(out_path_excel, index=False, engine="openpyxl")
    else:
        with open(out_path_json, "w", encoding="utf-8") as f_out:
            json.dump({"order": 0}, f_out, ensure_ascii=False)

        df = pd.read_json(out_path_json)
        df.to_excel(out_path_excel, index=True, engine="openpyxl")


def main() -> None:
    access_token, token_type = get_access_token()

    # get_all_orders_write_into_json(access_token, token_type)
    get_today_orders_write_into_excel(access_token, token_type)

if __name__ == "__main__":
    main()
