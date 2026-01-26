#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ozon Seller API + MoySklad -> Google Sheets

ВНИМАНИЕ: колонки O, P, U, V, W — запрещены к очистке/редактированию скриптом.
Скрипт пишет только:
- A..N (основные данные)
- Q (Комиссия FBO)
- R (Базовая логистика)

Структура A..N:
A Cabinet
B Категория товара нижнего уровня
C Тип товара
D SKU
E Название товара (МойСклад)
F offer_id
G Остаток (FBS+FBO суммарно)
H Закупочная цена
I Цена до скидок
J Минимальная цена
K Ваша цена
L Цена для покупателя
M Комиссия FBS
N Логистика FBS

Q Комиссия FBO
R Базовая логистика
"""

import os
import json
import time
import math
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


def extract_fbs_commission(info: dict):
    commissions = info.get("commissions") or []
    for c in commissions:
        if c.get("sale_schema") == "FBS":
            percent_raw = c.get("percent")
            try:
                percent = float(percent_raw) / 100 if percent_raw is not None else None
            except Exception:
                percent = None

            logistics = c.get("return_amount")
            try:
                logistics = round(float(logistics)) if logistics is not None else None
            except Exception:
                logistics = None

            return percent, logistics
    return None, None


def extract_fbo_commission_percent(info: dict) -> Optional[float]:
    commissions = info.get("commissions") or []
    for c in commissions:
        if c.get("sale_schema") == "FBO":
            percent_raw = c.get("percent")
            try:
                return float(percent_raw) / 100 if percent_raw is not None else None
            except Exception:
                return None
    return None


def extract_fbo_base_logistics(info: dict) -> Optional[int]:
    commissions = info.get("commissions") or []
    for c in commissions:
        if c.get("sale_schema") == "FBO":
            v = c.get("return_amount")
            try:
                return int(math.floor(float(v))) if v is not None else None
            except Exception:
                return None
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


def ms_get_by_href(ms_token: str, href: str, params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    if href.startswith(MS_BASE):
        path = href[len(MS_BASE):]
    else:
        path = href
    return ms_get(ms_token, path, params=params, timeout=timeout)


# ---------- MoySklad list/cache ----------

def ms_list_all(ms_token: str, path: str, *, limit: int = 1000, filters: Optional[str] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"limit": limit, "offset": offset}
        if filters:
            params["filter"] = filters
        data = ms_get(ms_token, path, params=params)
        chunk_rows = data.get("rows") or []
        rows.extend(chunk_rows)
        if len(chunk_rows) < limit:
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


# ---------- Ozon fetchers ----------

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


def fetch_ozon_stocks_by_offer_ids(client_id: str, api_key: str, offer_ids: List[str]) -> Dict[str, int]:
    """
    /v4/product/info/stocks
    В реальности API может возвращать items либо на верхнем уровне, либо в result.items.
    Суммируем present по типам fbs + fbo.
    """
    out: Dict[str, int] = {}
    if not offer_ids:
        return out

    for batch in chunk(offer_ids, 1000):
        payload = {"filter": {"offer_id": batch}, "limit": 1000}
        res = ozon_post(client_id, api_key, "/v4/product/info/stocks", payload)

        # ВАЖНО: поддерживаем оба формата ответа
        items = (res.get("result") or {}).get("items") or res.get("items") or []
        if not isinstance(items, list):
            items = []

        for it in items:
            oid = normalize_offer_id(it.get("offer_id"))
            if not oid:
                continue

            total = 0
            stocks = it.get("stocks") or []
            if not isinstance(stocks, list):
                out[oid] = 0
                continue

            for s in stocks:
                if not isinstance(s, dict):
                    continue
                stype = str(s.get("type") or "").lower()
                if stype not in ("fbs", "fbo"):
                    continue

                v = s.get("present")
                try:
                    total += int(float(v or 0))
                except Exception:
                    pass

            out[oid] = total

    return out

def _oz_price_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    v = float(x)
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
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

        p = _oz_price_str(it.get("price"))
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


# ---------- MoySklad: bundles buy price from components ----------

_bundle_buy_cache: Dict[str, Optional[float]] = {}


def ms_calc_bundle_buy_price(ms_token: str, ms_item: Dict[str, Any]) -> Optional[float]:
    meta = (ms_item or {}).get("meta") or {}
    href = meta.get("href")
    if not href:
        return None

    if href in _bundle_buy_cache:
        return _bundle_buy_cache[href]

    try:
        b = ms_get_by_href(ms_token, href, params={"expand": "components.assortment"})
    except Exception:
        _bundle_buy_cache[href] = None
        return None

    components_obj = b.get("components") or []
    if isinstance(components_obj, dict):
        components = components_obj.get("rows") or []
    elif isinstance(components_obj, list):
        components = components_obj
    else:
        components = []

    total = 0.0

    for c in components:
        if not isinstance(c, dict):
            continue

        qty = c.get("quantity")
        try:
            qty = float(qty) if qty is not None else 0.0
        except Exception:
            qty = 0.0

        assortment = c.get("assortment") or {}
        buy_val = ((assortment.get("buyPrice") or {}).get("value"))
        bp = money_from_ms(buy_val)

        if bp is None or qty <= 0:
            continue

        total += bp * qty

    total_out = total if total > 0 else None
    _bundle_buy_cache[href] = total_out
    return total_out


def fetch_ms_products_by_articles(ms_token: str, offer_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    cache_path = os.environ.get("MS_CACHE_PATH", "/root/google_ozon_prices/.cache/ms_catalog.json")
    ttl_sec = int(os.environ.get("MS_CACHE_TTL_SECONDS", "900"))

    cached = ms_load_catalog_cache(cache_path, ttl_sec)
    if cached is not None:
        return {oid: cached.get(oid, {}) for oid in offer_ids}

    filters = "archived=false"
    products = ms_list_all(ms_token, "/entity/product", filters=filters)
    bundles = ms_list_all(ms_token, "/entity/bundle", filters=filters)

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
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=30)
    return ws


def read_existing_sheet_prices(ws) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], Dict[str, Optional[float]]]]:
    """
    Читаем цены из таблицы для "старых" товаров.
    С учётом добавленной колонки Остаток:
    F=offer_id, I=Цена до скидок, J=Минимальная, K=Ваша цена
    """
    CAB_COL = 1        # A
    OFFER_COL = 6      # F
    OLD_COL = 9        # I (Цена до скидок)
    MIN_COL = 10       # J (Минимальная)
    YOUR_COL = 11      # K (Ваша)
    BUYER_COL = 12     # L (Цена для покупателя)

    values = ws.get_all_values()
    existing_keys: Set[Tuple[str, str]] = set()
    existing_prices: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}

    # Заголовок у тебя во 2-й строке. Мы просто читаем всё и фильтруем по cab+offer_id.
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
            "buyer_price": _cell_to_number(get(BUYER_COL)),
        }

    return existing_keys, existing_prices


def write_rows_to_sheet(
    ws,
    header: List[str],
    rows_a_to_n: List[List[Any]],
    col_q_values: List[List[Any]],
    col_r_values: List[List[Any]],
) -> None:
    """
    Пишем A..H + K..N + Q + R.
    I и J НЕ ТРОГАЕМ (чтобы формулы не удалялись).
    O, P, U, V, W — тоже НЕ трогаем.
    """

    # чистим только то, что реально перезаписываем
    ws.batch_clear(["A3:H"])
    ws.batch_clear(["K3:K"])
    ws.batch_clear(["M3:N"])
    ws.batch_clear(["Q3:Q"])
    ws.batch_clear(["R3:R"])

    # A..H
    left_header = header[0:8]  # A..H
    left_rows = [r[0:8] for r in rows_a_to_n]
    ws.update(
        range_name="A2:H",
        values=[left_header] + left_rows,
        value_input_option="USER_ENTERED",
    )

    # K..N (пропускаем I,J)
    right_header = header[10:14]  # K..N
    right_rows = [r[10:14] for r in rows_a_to_n]
    ws.update(
        range_name="K2:K",
        values=[[header[10]]] + [[r[10]] for r in rows_a_to_n],
        value_input_option="USER_ENTERED",
    )

    # Q, R как было
    ws.update(
        range_name="Q2",
        values=[["% FBO"]] + col_q_values,
        value_input_option="USER_ENTERED",
    )
    ws.update(
        range_name="R2",
        values=[["Баз лог"]] + col_r_values,
        value_input_option="USER_ENTERED",
    )
    ws.update(
        range_name="M2:N",
        values=[header[12:14]] + [r[12:14] for r in rows_a_to_n],
        value_input_option="USER_ENTERED",
    )

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

    # PUSH в Ozon только если явно включено env PUSH_PRICE=1
    if push_price and existing_offer_ids:
        to_push: List[Dict[str, Any]] = []

        for oid in existing_offer_ids:
            sheet_p = existing_prices.get((cab_label, oid), {}) or {}
            sheet_old = sheet_p.get("old_price")
            sheet_min = sheet_p.get("min_price")
            sheet_your = sheet_p.get("your_price")

            # Больше НЕ читаем цены из Ozon. Таблица (I,J,K) — единственный источник правды.
            # Значит, при PUSH отправляем то, что заполнено в таблице.
            row: Dict[str, Any] = {"offer_id": oid}
            any_value = False

            if sheet_old is not None:
                row["old_price"] = sheet_old
                any_value = True
            if sheet_min is not None:
                row["min_price"] = sheet_min
                any_value = True
            if sheet_your is not None:
                row["price"] = sheet_your
                any_value = True

            if any_value:
                to_push.append(row)

        if to_push:
            ozon_import_prices(client_id, api_key, to_push)
            print(f"{cab_label}: pushed prices for {len(to_push)} items (of {len(existing_offer_ids)})")
        else:
            print(f"{cab_label}: nothing to push (no filled I/J/K) (of {len(existing_offer_ids)})")

    # Цены из Ozon больше не подтягиваем (только остатки/комиссии/и т.д.)
    stocks_map = fetch_ozon_stocks_by_offer_ids(client_id, api_key, offer_ids)

    category_map, type_map = fetch_ozon_tree_maps(client_id, api_key)
    ms_map = fetch_ms_products_by_articles(ms_token, offer_ids)

    rows: List[Dict[str, Any]] = []

    for oid in offer_ids:
        key = (cab_label, oid)

        info = info_map.get(oid, {})
        fbs_commission_percent, fbs_logistics = extract_fbs_commission(info)
        fbo_commission_percent = extract_fbo_commission_percent(info)
        fbo_base_logistics = extract_fbo_base_logistics(info)

        stock_total = stocks_map.get(oid, 0)

        ms = ms_map.get(oid, {})

        dcid = info.get("description_category_id")
        tid = info.get("type_id")

        category_name = category_map.get(int(dcid), "") if isinstance(dcid, int) else ""
        type_name = type_map.get(int(tid), "") if isinstance(tid, int) else ""

        sku = info.get("sku")
        try:
            sku = int(sku) if sku is not None else None
        except Exception:
            sku = None

        ms_name = ms.get("name", "") if isinstance(ms, dict) else ""
        buy_price = money_from_ms((ms.get("buyPrice") or {}).get("value") if isinstance(ms, dict) else None)

        if buy_price is None and isinstance(ms, dict):
            meta = ms.get("meta") or {}
            if meta.get("type") == "bundle":
                buy_price = ms_calc_bundle_buy_price(ms_token, ms)

        if key in existing_prices:
            old_price = existing_prices[key].get("old_price")
            min_price = existing_prices[key].get("min_price")
            your_price = existing_prices[key].get("your_price")
            buyer_price = existing_prices[key].get("buyer_price")
        else:
            # Новый товар в таблице: цены не подтягиваем из Ozon, оставляем пусто.
            old_price = None
            min_price = None
            your_price = None
            buyer_price = None

        rows.append({
            "cab": cab_label,
            "category": category_name,
            "type": type_name,
            "sku": sku,
            "ms_name": ms_name,
            "offer_id": oid,
            "stock": stock_total,
            "buy_price": buy_price,
            "old_price": old_price,
            "min_price": min_price,
            "your_price": your_price,
            "buyer_price": buyer_price,
            "fbs_commission_percent": fbs_commission_percent,
            "fbs_logistics": fbs_logistics,
            "fbo_commission_percent": fbo_commission_percent,
            "fbo_base_logistics": fbo_base_logistics,
        })

    return rows


def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def norm(s: Any) -> str:
        return (str(s) if s is not None else "").strip().lower()

    return sorted(
        rows,
        key=lambda r: (norm(r.get("category")), norm(r.get("type")), norm(r.get("ms_name")), norm(r.get("offer_id"))),
    )


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
        raise SystemExit("SPREADSHEET_ID is required")
    if not service_account_json or not os.path.exists(service_account_json):
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
        "Каб",
        "Категория",
        "Тип",
        "SKU",
        "Название",
        "offer_id",
        "Остаток",
        "Закуп",
        "Цена до",
        "Мин цена",
        "Цена Oz",
        "Цена налог",
        "% FBS",
        "Лог FBS",
    ]

    sheet_rows: List[List[Any]] = []
    col_q_values: List[List[Any]] = []
    col_r_values: List[List[Any]] = []

    for r in all_rows:
        offer_id_text = "'" + str(r.get("offer_id", "")).strip()

        sheet_rows.append([
            r.get("cab", ""),
            r.get("category", ""),
            r.get("type", ""),
            r.get("sku", ""),
            r.get("ms_name", ""),
            offer_id_text,
            r.get("stock", 0),
            r.get("buy_price", ""),
            r.get("old_price", ""),
            r.get("min_price", ""),
            r.get("your_price", ""),
            r.get("buyer_price", ""),
            r.get("fbs_commission_percent", ""),
            r.get("fbs_logistics", ""),
        ])

        col_q_values.append([r.get("fbo_commission_percent", "")])
        col_r_values.append([r.get("fbo_base_logistics", "")])

    write_rows_to_sheet(
        ws,
        header,
        sheet_rows,
        col_q_values,
        col_r_values,
    )

    print(f"Done. Written {len(sheet_rows)} rows to '{worksheet_name}'.")


if __name__ == "__main__":
    main()
