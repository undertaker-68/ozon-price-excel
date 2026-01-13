#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Manual sync: Ozon Seller API + MoySklad -> Google Sheets.

Run:
  pip install -r requirements.txt
  copy config.example.env -> .env and fill values
  python sync.py
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

OZON_BASE = "https://api-seller.ozon.ru"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

HEADERS_MS_ACCEPT = "application/json;charset=utf-8"


def chunk(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def ozon_post(client_id: str, api_key: str, path: str, payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    url = f"{OZON_BASE}{path}"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"Ozon {path} failed {resp.status_code}: {resp.text}")
    return resp.json()


def ms_get(ms_token, path, params=None, timeout=60):
    url = MS_BASE + path
    headers = {
        "Authorization": f"Bearer {ms_token}",
        "Accept": "application/json;charset=utf-8",
    }

    # ретраи: 429 (лимиты) + временные 5xx
    attempts = 8
    for attempt in range(1, attempts + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)

        if resp.status_code == 200:
            return resp.json()

        # MoySklad rate limit
        if resp.status_code == 429:
            # MoySklad часто присылает интервалы в заголовках
            retry_ms = resp.headers.get("X-Lognex-Retry-TimeInterval") or resp.headers.get("X-Lognex-Retry-After")
            if retry_ms:
                try:
                    sleep_s = max(1.0, float(retry_ms) / 1000.0)
                except Exception:
                    sleep_s = 3.0
            else:
                # запасной бэкофф
                sleep_s = min(30.0, 2.0 * attempt)

            print(f"MoySklad 429 rate limit, sleep {sleep_s:.1f}s (attempt {attempt}/{attempts})")
            time.sleep(sleep_s)
            continue

        # временные ошибки
        if resp.status_code in (500, 502, 503, 504):
            sleep_s = min(30.0, 2.0 * attempt)
            print(f"MoySklad {resp.status_code}, sleep {sleep_s:.1f}s (attempt {attempt}/{attempts})")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"MoySklad {path} failed {resp.status_code}: {resp.text}")

    raise RuntimeError(f"MoySklad {path} failed after {attempts} attempts (last status {resp.status_code}): {resp.text}")

def fetch_ozon_tree_maps(client_id: str, api_key: str) -> Tuple[Dict[int, str], Dict[int, str]]:
    """Returns (category_id->name, type_id->name) parsed from /v1/description-category/tree."""
    data = ozon_post(client_id, api_key, "/v1/description-category/tree", {"language": "RU"})

    category_map: Dict[int, str] = {}
    type_map: Dict[int, str] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            # Common keys: description_category_id/category_name, type_id/type_name, children
            dcid = node.get("description_category_id")
            cname = node.get("category_name")
            if isinstance(dcid, int) and isinstance(cname, str):
                category_map[dcid] = cname

            tid = node.get("type_id")
            tname = node.get("type_name")
            if isinstance(tid, int) and isinstance(tname, str):
                type_map[tid] = tname

            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)

    walk(data)
    return category_map, type_map


def fetch_ozon_product_list(client_id: str, api_key: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    last_id = ""
    while True:
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        res = ozon_post(client_id, api_key, "/v3/product/list", payload)
        page = res.get("result", {}).get("items", [])
        if not page:
            break
        items.extend(page)
        new_last_id = res.get("result", {}).get("last_id", "")
        if not new_last_id or new_last_id == last_id:
            break
        last_id = new_last_id
    return items


def fetch_ozon_info_by_product_ids(client_id: str, api_key: str, product_ids: List[int]) -> Dict[str, Dict[str, Any]]:
    """Returns offer_id -> info item (contains description_category_id, type_id, old_price, min_price, price as strings)."""
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunk(product_ids, 50):
        payload = {"product_id": [str(x) for x in batch]}
        res = ozon_post(client_id, api_key, "/v3/product/info/list", payload)
        for it in res.get("items", []):
            offer_id = it.get("offer_id")
            if offer_id:
                out[str(offer_id)] = it
    return out


def fetch_ozon_prices_by_offer_ids(client_id: str, api_key: str, offer_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Returns offer_id -> price block from /v5/product/info/prices."""
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunk(offer_ids, 1000):
        payload = {"filter": {"offer_id": batch}, "last_id": "", "limit": 1000}
        res = ozon_post(client_id, api_key, "/v5/product/info/prices", payload)
        for it in res.get("items", []):
            offer_id = it.get("offer_id")
            price = it.get("price", {})
            if offer_id:
                out[str(offer_id)] = price
    return out


def fetch_ms_products_by_articles(ms_token: str, articles: List[str]) -> Dict[str, Dict[str, Any]]:
    """Returns article -> MS product row.

    MoySklad API doesn't support a simple IN filter by article, so we request per article.
    For 186 products this is still manageable; we use a small delay to be polite.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for idx, art in enumerate(articles, start=1):
        params = {"filter": f"article={art}"}
        data = ms_get(ms_token, "/entity/product", params=params)
        rows = data.get("rows", [])
        if rows:
            out[art] = rows[0]
        # simple pacing
        if idx % 20 == 0:
            time.sleep(0.25)
        else:
            time.sleep(0.05)
    return out


def money_from_ms(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) / 100.0
    except Exception:
        return None


def money_from_ozon(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def connect_sheet(service_account_json: str, spreadsheet_id: str, worksheet_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)
    return ws


def write_rows_to_sheet(ws, header: List[str], rows: List[List[Any]]) -> None:
    # Clear then update
    ws.clear()
    values = [header] + rows
    ws.update(values, value_input_option="USER_ENTERED")


def build_rows_for_cabinet(
    cab_label: str,
    client_id: str,
    api_key: str,
    ms_token: str,
) -> List[Dict[str, Any]]:
    # 1) list products
    prod_items = fetch_ozon_product_list(client_id, api_key)
    offer_ids = [str(x.get("offer_id")) for x in prod_items if x.get("offer_id")]
    product_ids = [int(x.get("product_id")) for x in prod_items if x.get("product_id") is not None]

    # 2) info list (gives category/type ids, and string prices)
    info_map = fetch_ozon_info_by_product_ids(client_id, api_key, product_ids)

    # 3) prices list (numbers, includes marketing_seller_price)
    prices_map = fetch_ozon_prices_by_offer_ids(client_id, api_key, offer_ids)

    # 4) category/type name dictionaries
    category_map, type_map = fetch_ozon_tree_maps(client_id, api_key)

    # 5) MS data by article (offer_id)
    ms_map = fetch_ms_products_by_articles(ms_token, offer_ids)

    # 6) build flat rows
    result: List[Dict[str, Any]] = []
    for offer_id in offer_ids:
        info = info_map.get(offer_id, {})
        price = prices_map.get(offer_id, {})
        ms = ms_map.get(offer_id, {})

        dcid = info.get("description_category_id")
        tid = info.get("type_id")

        category_name = category_map.get(int(dcid), "") if isinstance(dcid, int) else ""
        type_name = type_map.get(int(tid), "") if isinstance(tid, int) else ""

        ms_name = ms.get("name", "") if isinstance(ms, dict) else ""
        buy_price = money_from_ms((ms.get("buyPrice") or {}).get("value") if isinstance(ms, dict) else None)

        old_price = money_from_ozon(price.get("old_price"))
        min_price = money_from_ozon(price.get("min_price"))
        your_price = money_from_ozon(price.get("marketing_seller_price"))
        buyer_price = money_from_ozon(price.get("price"))

        row = {
            "cab": cab_label,
            "category": category_name,
            "type": type_name,
            "ms_name": ms_name,
            "offer_id": offer_id,
            "buy_price": buy_price,
            "old_price": old_price,
            "min_price": min_price,
            "your_price": your_price,
            "buyer_price": buyer_price,
        }
        result.append(row)

    return result


def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Cabinet is NOT a sort key per your rule; it is just a label.
    def norm(s: Any) -> str:
        return (str(s) if s is not None else "").strip().lower()

    return sorted(
        rows,
        key=lambda r: (
            norm(r.get("category")),
            norm(r.get("type")),
            norm(r.get("ms_name")),
            norm(r.get("offer_id")),
        ),
    )


def main() -> None:
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("WORKSHEET_NAME", "API Ozon").strip()
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    ms_token = os.getenv("MS_TOKEN", "").strip()

    if not spreadsheet_id:
        raise SystemExit("SPREADSHEET_ID is required (see config.example.env)")
    if not service_account_json:
        raise SystemExit("GOOGLE_SERVICE_ACCOUNT_JSON is required (see config.example.env)")
    if not os.path.exists(service_account_json):
        raise SystemExit(f"Service account JSON not found: {service_account_json}")
    if not ms_token:
        raise SystemExit("MS_TOKEN is required")

    cab1_id = os.getenv("OZON_CLIENT_ID_1", "").strip()
    cab1_key = os.getenv("OZON_API_KEY_1", "").strip()
    cab2_id = os.getenv("OZON_CLIENT_ID_2", "").strip()
    cab2_key = os.getenv("OZON_API_KEY_2", "").strip()

    if not cab1_id or not cab1_key:
        raise SystemExit("OZON_CLIENT_ID_1 and OZON_API_KEY_1 are required")

    all_rows: List[Dict[str, Any]] = []

    print("Sync Cab1...")
    all_rows.extend(build_rows_for_cabinet("Cab1", cab1_id, cab1_key, ms_token))

    if cab2_id and cab2_key:
        print("Sync Cab2...")
        all_rows.extend(build_rows_for_cabinet("Cab2", cab2_id, cab2_key, ms_token))

    all_rows = sort_rows(all_rows)

    header = [
        "Cabinet",
        "Категория товара нижнего уровня",
        "Тип товара",
        "Название товара (МойСклад)",
        "offer_id",
        "Закупочная цена",
        "Цена до скидок",
        "Минимальная цена",
        "Ваша цена",
        "Цена для покупателя",
    ]

    sheet_rows: List[List[Any]] = []
    for r in all_rows:
        sheet_rows.append(
            [
                r.get("cab", ""),
                r.get("category", ""),
                r.get("type", ""),
                r.get("ms_name", ""),
                r.get("offer_id", ""),
                r.get("buy_price", ""),
                r.get("old_price", ""),
                r.get("min_price", ""),
                r.get("your_price", ""),
                r.get("buyer_price", ""),
            ]
        )

    ws = connect_sheet(service_account_json, spreadsheet_id, worksheet_name)
    write_rows_to_sheet(ws, header, sheet_rows)

    print(f"Done. Written {len(sheet_rows)} rows to '{worksheet_name}'.")


if __name__ == "__main__":
    main()
