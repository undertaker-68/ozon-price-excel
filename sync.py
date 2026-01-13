#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Manual sync: Ozon Seller API + MoySklad -> Google Sheets.

Таблица:
A=1 cab
E=5 offer_id
G=7 old_price   (Цена до скидок)
H=8 min_price   (Минимальная цена)
I=9 your_price  (Ваша цена)
J=10 buyer_price (Цена для покупателя)

Логика:
- old_price/min_price/your_price:
  * если товар уже есть в таблице (cab+offer_id) -> берем из таблицы
  * если товар новый -> тянем из Ozon (/v5/product/info/prices)
- buyer_price всегда тянем из Ozon
- PUSH_PRICE=1 -> для "старых" товаров пушим цены из таблицы в Ozon (/v1/product/import/prices)
- offer_id нормализуем (цифры <5 -> zfill(5)), в Sheets пишем как текст (с апострофом)
"""

import os
import json, time
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

OZON_BASE = "https://api-seller.ozon.ru"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
MS_ACCEPT = "application/json;charset=utf-8"


# ---------- helpers ----------

def chunk(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def normalize_offer_id(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    # если пришло из Sheets с апострофом
    s = s.lstrip("'").strip()
    if s.isdigit() and len(s) < 5:
        s = s.zfill(5)
    return s


def _cell_to_number(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().replace(" ", "").replace("\u00A0", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None

def _price_norm(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def _price_changed(a: Any, b: Any, eps: float = 0.01) -> bool:
    """
    True если цена отличается. eps=0.01 для рублёвых цен.
    """
    aa = _price_norm(a)
    bb = _price_norm(b)
    if aa is None or bb is None:
        return False
    return abs(aa - bb) > eps


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


# ---------- HTTP wrappers ----------

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


def ms_get(ms_token: str, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    url = f"{MS_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {ms_token}",
        "Accept": MS_ACCEPT,
    }

    attempts = 8
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.SSLError):
            sleep_s = min(30.0, 2.0 * attempt)
            print(f"MoySklad network timeout/ssl, sleep {sleep_s:.1f}s (attempt {attempt}/{attempts})")
            time.sleep(sleep_s)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            retry_ms = resp.headers.get("X-Lognex-Retry-TimeInterval") or resp.headers.get("X-Lognex-Retry-After")
            if retry_ms:
                try:
                    sleep_s = max(1.0, float(retry_ms) / 1000.0)
                except Exception:
                    sleep_s = 3.0
            else:
                sleep_s = min(30.0, 2.0 * attempt)
            print(f"MoySklad 429 rate limit, sleep {sleep_s:.1f}s (attempt {attempt}/{attempts})")
            time.sleep(sleep_s)
            continue

        if resp.status_code in (500, 502, 503, 504):
            sleep_s = min(30.0, 2.0 * attempt)
            print(f"MoySklad {resp.status_code}, sleep {sleep_s:.1f}s (attempt {attempt}/{attempts})")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"MoySklad {path} failed {resp.status_code}: {resp.text}")

    raise RuntimeError(f"MoySklad {path} failed after {attempts} attempts")


# ---------- Ozon fetchers ----------

def ms_list_all(ms_token: str, path: str, *, limit: int = 1000, filters: Optional[str] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"limit": limit, "offset": offset}
        if filters:
            params["filter"] = filters
        data = ms_get(ms_token, path, params=params)
        chunk = data.get("rows") or []
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
    return rows

def ms_load_catalog_cache(cache_path: str, ttl_sec: int) -> Optional[Dict[str, Dict[str, Any]]]:
    try:
        st = os.stat(cache_path)
        if time.time() - st.st_mtime > ttl_sec:
            return None
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def ms_save_catalog_cache(cache_path: str, data: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def fetch_ozon_tree_maps(client_id: str, api_key: str) -> Tuple[Dict[int, str], Dict[int, str]]:
    data = ozon_post(client_id, api_key, "/v1/description-category/tree", {"language": "RU"})

    category_map: Dict[int, str] = {}
    type_map: Dict[int, str] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
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
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunk(product_ids, 50):
        payload = {"product_id": [str(x) for x in batch]}
        res = ozon_post(client_id, api_key, "/v3/product/info/list", payload)
        for it in res.get("items", []):
            offer_id = it.get("offer_id")
            if offer_id is not None:
                out[normalize_offer_id(offer_id)] = it
    return out


def fetch_ozon_prices_by_offer_ids(client_id: str, api_key: str, offer_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not offer_ids:
        return out
    for batch in chunk(offer_ids, 1000):
        payload = {"filter": {"offer_id": batch}, "last_id": "", "limit": 1000}
        res = ozon_post(client_id, api_key, "/v5/product/info/prices", payload)
        for it in res.get("items", []):
            oid = it.get("offer_id")
            price = it.get("price", {})
            if oid is not None:
                out[normalize_offer_id(oid)] = price
    return out


def _oz_price_str(x: Any) -> Optional[str]:
    # Ozon хочет строки. В таблице могут быть 2937 или 2937.0
    if x is None:
        return None
    v = float(x)
    # если целое — отправляем без .0
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    # иначе 2 знака после запятой
    return f"{v:.2f}".replace(",", ".")


def ozon_import_prices(client_id: str, api_key: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    url = f"{OZON_BASE}/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {"prices": []}

    for it in items:
        offer_id = normalize_offer_id(it.get("offer_id"))
        if not offer_id:
            continue

        p  = _oz_price_str(it.get("price"))
        op = _oz_price_str(it.get("old_price"))
        mp = _oz_price_str(it.get("min_price"))

        if p is None and op is None and mp is None:
            continue

        row: Dict[str, Any] = {"offer_id": offer_id}
        if p is not None:
            row["price"] = p
        if op is not None:
            row["old_price"] = op
        if mp is not None:
            row["min_price"] = mp

        payload["prices"].append(row)

    if not payload["prices"]:
        return {"skipped": True, "reason": "no prices to push"}

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Ozon import prices failed {resp.status_code}: {resp.text}")
    return resp.json()

# ---------- MoySklad fetchers ----------

def fetch_ms_products_by_articles(ms_token: str, offer_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    # параметры кеша
    cache_path = os.environ.get("MS_CACHE_PATH", "/root/google_ozon_prices/.cache/ms_catalog.json")
    ttl_sec = int(os.environ.get("MS_CACHE_TTL_SECONDS", "900"))  # 15 минут

    cached = ms_load_catalog_cache(cache_path, ttl_sec)
    if cached is not None:
        # cached = { "00022": {...}, ... }
        return {oid: cached.get(oid, {}) for oid in offer_ids}

    # 1) забираем все products и bundles (можно ограничить archived=false)
    # если хочешь включать архив: убери filters
    filters = "archived=false"
    products = ms_list_all(ms_token, "/entity/product", filters=filters)
    bundles  = ms_list_all(ms_token, "/entity/bundle",  filters=filters)

    catalog: Dict[str, Dict[str, Any]] = {}

    for it in products:
        art = it.get("article")
        if art:
            catalog[str(art)] = it

    for it in bundles:
        art = it.get("article")
        if art and art not in catalog:
            catalog[str(art)] = it

    ms_save_catalog_cache(cache_path, catalog)

    return {oid: catalog.get(oid, {}) for oid in offer_ids}

# ---------- Google Sheets ----------

def connect_sheet(service_account_json: str, spreadsheet_id: str, worksheet_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=20)
    return ws


def read_existing_sheet_prices(ws) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], Dict[str, Optional[float]]]]:
    CAB_COL = 1
    OFFER_COL = 5
    OLD_COL = 7
    MIN_COL = 8
    YOUR_COL = 9

    values = ws.get_all_values()
    existing_keys: Set[Tuple[str, str]] = set()
    existing_prices: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}

    for row in values[1:]:
        def get(col1: int) -> Any:
            return row[col1 - 1] if len(row) >= col1 else ""

        cab = str(get(CAB_COL)).strip()
        offer_id = normalize_offer_id(get(OFFER_COL))

        if not cab or not offer_id:
            continue

        key = (cab, offer_id)
        existing_keys.add(key)
        existing_prices[key] = {
            "old_price": _cell_to_number(get(OLD_COL)),
            "min_price": _cell_to_number(get(MIN_COL)),
            "your_price": _cell_to_number(get(YOUR_COL)),
        }

    return existing_keys, existing_prices


def write_rows_to_sheet(ws, header: List[str], rows: List[List[Any]]) -> None:
    ws.clear()
    ws.update([header] + rows, value_input_option="USER_ENTERED")


# ---------- build rows ----------

def build_rows_for_cabinet(
    cab_label: str,
    client_id: str,
    api_key: str,
    ms_token: str,
    existing_keys: Set[Tuple[str, str]],
    existing_prices: Dict[Tuple[str, str], Dict[str, Optional[float]]],
    push_price: bool,
) -> List[Dict[str, Any]]:

    prod_items = fetch_ozon_product_list(client_id, api_key)

    offer_ids = [normalize_offer_id(x.get("offer_id")) for x in prod_items if x.get("offer_id") is not None]
    offer_ids = [oid for oid in offer_ids if oid]

    product_ids = [int(x.get("product_id")) for x in prod_items if x.get("product_id") is not None]

    info_map = fetch_ozon_info_by_product_ids(client_id, api_key, product_ids)

    existing_offer_ids = [oid for oid in offer_ids if (cab_label, oid) in existing_keys]
    new_offer_ids = [oid for oid in offer_ids if (cab_label, oid) not in existing_keys]

        # PUSH в Ozon только для изменённых "старых"
    if push_price and existing_offer_ids:
        # тянем текущие цены Ozon для сравнения (одним запросом)
        oz_existing = fetch_ozon_prices_by_offer_ids(client_id, api_key, existing_offer_ids)

        to_push: List[Dict[str, Any]] = []
        changed_cnt = 0

        for oid in existing_offer_ids:
            sheet_p = existing_prices.get((cab_label, oid), {}) or {}

            sheet_old  = sheet_p.get("old_price")
            sheet_min  = sheet_p.get("min_price")
            sheet_your = sheet_p.get("your_price")

            oz_p = oz_existing.get(oid, {}) or {}
            oz_old  = money_from_ozon(oz_p.get("old_price"))
            oz_min  = money_from_ozon(oz_p.get("min_price"))
            oz_your = money_from_ozon(oz_p.get("marketing_seller_price"))

            row: Dict[str, Any] = {"offer_id": oid}
            any_change = False

            # отправляем только непустое и только если отличается
            if sheet_old is not None and _price_changed(sheet_old, oz_old):
                row["old_price"] = sheet_old
                any_change = True

            if sheet_min is not None and _price_changed(sheet_min, oz_min):
                row["min_price"] = sheet_min
                any_change = True

            if sheet_your is not None and _price_changed(sheet_your, oz_your):
                row["price"] = sheet_your
                any_change = True

            if any_change:
                to_push.append(row)
                changed_cnt += 1

        if to_push:
            try:
                ozon_import_prices(client_id, api_key, to_push)
                print(f"{cab_label}: pushed changed prices for {changed_cnt} items (of {len(existing_offer_ids)})")
            except Exception as e:
                print(f"{cab_label}: FAILED to push changed prices: {e}")
        else:
            print(f"{cab_label}: no price changes to push (of {len(existing_offer_ids)})")

    # цены тянем из Ozon только для новых
    prices_map_new = fetch_ozon_prices_by_offer_ids(client_id, api_key, new_offer_ids)

    # buyer_price тянем для всех
    prices_map_all = fetch_ozon_prices_by_offer_ids(client_id, api_key, offer_ids)

    category_map, type_map = fetch_ozon_tree_maps(client_id, api_key)
    ms_map = fetch_ms_products_by_articles(ms_token, offer_ids)

    rows: List[Dict[str, Any]] = []

    for oid in offer_ids:
        key = (cab_label, oid)

        info = info_map.get(oid, {})
        ms = ms_map.get(oid, {})

        dcid = info.get("description_category_id")
        tid = info.get("type_id")

        category_name = category_map.get(int(dcid), "") if isinstance(dcid, int) else ""
        type_name = type_map.get(int(tid), "") if isinstance(tid, int) else ""

        ms_name = ms.get("name", "") if isinstance(ms, dict) else ""
        buy_price = money_from_ms((ms.get("buyPrice") or {}).get("value") if isinstance(ms, dict) else None)

        # 3 поля цен
        if key in existing_prices:
            old_price = existing_prices[key].get("old_price")
            min_price = existing_prices[key].get("min_price")
            your_price = existing_prices[key].get("your_price")
        else:
            pnew = prices_map_new.get(oid, {})
            old_price = money_from_ozon(pnew.get("old_price"))
            min_price = money_from_ozon(pnew.get("min_price"))
            your_price = money_from_ozon(pnew.get("marketing_seller_price"))

        # buyer_price всегда актуальная
        pall = prices_map_all.get(oid, {})
        buyer_price = money_from_ozon(pall.get("price"))

        rows.append({
            "cab": cab_label,
            "category": category_name,
            "type": type_name,
            "ms_name": ms_name,
            "offer_id": oid,
            "buy_price": buy_price,
            "old_price": old_price,
            "min_price": min_price,
            "your_price": your_price,
            "buyer_price": buyer_price,
        })

    return rows


def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def norm(s: Any) -> str:
        return (str(s) if s is not None else "").strip().lower()

    return sorted(
        rows,
        key=lambda r: (norm(r.get("category")), norm(r.get("type")), norm(r.get("ms_name")), norm(r.get("offer_id"))),
    )


# ---------- main ----------

def main() -> None:
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("WORKSHEET_NAME", "API Ozon").strip()
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    ms_token = os.getenv("MS_TOKEN", "").strip()

    cab1_id = os.getenv("OZON_CLIENT_ID_1", "").strip()
    cab1_key = os.getenv("OZON_API_KEY_1", "").strip()
    cab2_id = os.getenv("OZON_CLIENT_ID_2", "").strip()
    cab2_key = os.getenv("OZON_API_KEY_2", "").strip()

    push_price = os.getenv("PUSH_PRICE", "0").strip().lower() in ("1", "true", "yes", "y")

    if not spreadsheet_id:
        raise SystemExit("SPREADSHEET_ID is required (see config.example.env)")
    if not service_account_json:
        raise SystemExit("GOOGLE_SERVICE_ACCOUNT_JSON is required (see config.example.env)")
    if not os.path.exists(service_account_json):
        raise SystemExit(f"Service account JSON not found: {service_account_json}")
    if not ms_token:
        raise SystemExit("MS_TOKEN is required")
    if not cab1_id or not cab1_key:
        raise SystemExit("OZON_CLIENT_ID_1 and OZON_API_KEY_1 are required")

    ws = connect_sheet(service_account_json, spreadsheet_id, worksheet_name)
    existing_keys, existing_prices = read_existing_sheet_prices(ws)

    all_rows: List[Dict[str, Any]] = []

    print("Sync Cab1...")
    all_rows.extend(build_rows_for_cabinet("Cab1", cab1_id, cab1_key, ms_token, existing_keys, existing_prices, push_price))

    if cab2_id and cab2_key:
        print("Sync Cab2...")
        all_rows.extend(build_rows_for_cabinet("Cab2", cab2_id, cab2_key, ms_token, existing_keys, existing_prices, push_price))

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
        offer_id_text = "'" + str(r.get("offer_id", "")).strip()
        sheet_rows.append([
            r.get("cab", ""),
            r.get("category", ""),
            r.get("type", ""),
            r.get("ms_name", ""),
            offer_id_text,
            r.get("buy_price", ""),
            r.get("old_price", ""),
            r.get("min_price", ""),
            r.get("your_price", ""),
            r.get("buyer_price", ""),
        ])

    write_rows_to_sheet(ws, header, sheet_rows)
    print(f"Done. Written {len(sheet_rows)} rows to '{worksheet_name}'.")


if __name__ == "__main__":
    main()
