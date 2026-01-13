#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Manual sync: Ozon Seller API + MoySklad -> Google Sheets.

Logic:
- Columns "Цена до скидок" (old_price), "Минимальная цена" (min_price), "Ваша цена" (your_price):
  * If товар уже есть в таблице (cab+offer_id) -> берём эти 3 значения ИЗ ТАБЛИЦЫ (не тянем с Ozon)
  * If товар новый -> тянем эти 3 значения из Ozon
- "Цена для покупателя" всегда тянем с Ozon (динамическая)
- offer_id пишем как текст (с апострофом), и если offer_id чисто числовой и <5 символов -> zfill(5)
"""

import os
import time
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

OZON_BASE = "https://api-seller.ozon.ru"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
HEADERS_MS_ACCEPT = "application/json;charset=utf-8"


def chunk(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def normalize_offer_id(raw: Any) -> str:
    """Приводим offer_id к строке.
    Если строка только из цифр и длина < 5 -> дополняем ведущими нулями до 5.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if s.isdigit() and len(s) < 5:
        s = s.zfill(5)
    return s


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
        "Accept": HEADERS_MS_ACCEPT,
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


def fetch_ozon_tree_maps(client_id: str, api_key: str) -> Tuple[Dict[int, str], Dict[int, str]]:
    """Returns (category_id->name, type_id->name) parsed from /v1/description-category/tree."""
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
    """Returns offer_id -> info item (contains description_category_id, type_id, old_price/min_price/price as strings)."""
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
    """Returns offer_id -> price block from /v5/product/info/prices."""
    out: Dict[str, Dict[str, Any]] = {}
    if not offer_ids:
        return out
    for batch in chunk(offer_ids, 1000):
        payload = {"filter": {"offer_id": batch}, "last_id": "", "limit": 1000}
        res = ozon_post(client_id, api_key, "/v5/product/info/prices", payload)
        for it in res.get("items", []):
            offer_id = it.get("offer_id")
            price = it.get("price", {})
            if offer_id is not None:
                out[normalize_offer_id(offer_id)] = price
    return out

def ozon_import_prices(client_id: str, api_key: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Отправка цен в Ozon из таблицы.
    Эндпоинт: POST /v1/product/import/prices
    items: [{"offer_id": "...", "price": 1843, "old_price": 2188, "min_price": 1732}, ...]
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
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

        p = it.get("price")
        op = it.get("old_price")
        mp = it.get("min_price")

        # пропускаем полностью пустые
        if p is None and op is None and mp is None:
            continue

        # Важно: отправляем числа (рубли). Если Ozon потребует строки — поменяем тут в одном месте.
        row = {"offer_id": offer_id}
        if p is not None:
            row["price"] = int(p)
        if op is not None:
            row["old_price"] = int(op)
        if mp is not None:
            row["min_price"] = int(mp)

        payload["prices"].append(row)

    if not payload["prices"]:
        return {"skipped": True, "reason": "no prices to push"}

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Ozon import prices failed {resp.status_code}: {resp.text}")
    return resp.json()


def fetch_ms_products_by_articles(ms_token: str, articles: List[str]) -> Dict[str, Dict[str, Any]]:
    """Returns article -> MS entity row (product/bundle/variant).
    Priority: product > bundle > variant.
    """
    out: Dict[str, Dict[str, Any]] = {}

    for idx, art in enumerate(articles, start=1):
        params = {"filter": f"article={art}"}

        data = ms_get(ms_token, "/entity/product", params=params)
        rows = data.get("rows", [])
        if rows:
            out[art] = rows[0]
        else:
            data_b = ms_get(ms_token, "/entity/bundle", params=params)
            rows_b = data_b.get("rows", [])
            if rows_b:
                out[art] = rows_b[0]
            else:
                # У variant нет фильтра article; используем code
                data_v = ms_get(ms_token, "/entity/variant", params={"filter": f"code={art}"})
                rows_v = data_v.get("rows", [])
                if rows_v:
                    out[art] = rows_v[0]

        # pacing
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


def read_existing_sheet_prices(ws) -> Tuple[Set[Tuple[str, str]], Dict[Tuple[str, str], Dict[str, Optional[float]]]]:
    """
    Возвращает:
      existing_keys: set((cab, offer_id))
      existing_prices: dict((cab, offer_id) -> {"old_price":..., "min_price":..., "your_price":...})

    Колонки (1-based):
      cab=A=1, offer_id=E=5, old_price=G=7, min_price=H=8, your_price=I=9
    """
    CAB_COL = 1
    OFFER_COL = 5
    OLD_COL = 7
    MIN_COL = 8
    YOUR_COL = 9

    values = ws.get_all_values()
    existing_prices: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}
    existing_keys: Set[Tuple[str, str]] = set()

    for row in values[1:]:
        def get(col1: int) -> Any:
            return row[col1 - 1] if len(row) >= col1 else ""

        cab = str(get(CAB_COL)).strip()
        offer_id = normalize_offer_id(str(get(OFFER_COL)).strip().lstrip("'"))

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
    values = [header] + rows
    ws.update(values, value_input_option="USER_ENTERED")


def build_rows_for_cabinet(
    cab_label: str,
    client_id: str,
    api_key: str,
    ms_token: str,
    existing_prices: Dict[Tuple[str, str], Dict[str, int]],
    push_price: bool,
) -> List[Dict[str, Any]]:

    prod_items = fetch_ozon_product_list(client_id, api_key)

    offer_ids = [normalize_offer_id(x.get("offer_id")) for x in prod_items if x.get("offer_id") is not None]
    offer_ids = [oid for oid in offer_ids if oid]  # убрать пустые

    product_ids = [int(x.get("product_id")) for x in prod_items if x.get("product_id") is not None]

    # info list (category/type ids + some string prices, но мы цены отсюда НЕ используем)
    info_map = fetch_ozon_info_by_product_ids(client_id, api_key, product_ids)

    # новые offer_id (для них тянем 3 поля цен из Ozon)
    new_offer_ids = [oid for oid in offer_ids if (cab_label, oid) not in existing_keys]

        # 2.4) PUSH: для товаров, которые уже есть в таблице, отправляем цены в Ozon из таблицы
    if push_price and existing_offer_ids:
        to_push: List[Dict[str, Any]] = []
        for oid in existing_offer_ids:
            p = existing_prices.get((cab_label, oid), {})
            # Таблица хранит уже рубли (int). Если где-то пусто — просто не отправим это поле.
            to_push.append({
                "offer_id": oid,
                "old_price": p.get("old_price"),
                "min_price": p.get("min_price"),
                "price": p.get("your_price"),
            })

        try:
            res = ozon_import_prices(client_id, api_key, to_push)
            # Можно распечатать task_id/статус — зависит от ответа Ozon
            print(f"{cab_label}: pushed prices for {len(existing_offer_ids)} items")
        except Exception as e:
            print(f"{cab_label}: FAILED to push prices: {e}")

    # prices list только для новых
    prices_map_new = fetch_ozon_prices_by_offer_ids(client_id, api_key, new_offer_ids)

    # category/type names
    category_map, type_map = fetch_ozon_tree_maps(client_id, api_key)

    # MS by article
    ms_map = fetch_ms_products_by_articles(ms_token, offer_ids)

    result: List[Dict[str, Any]] = []
    for offer_id in offer_ids:
        key = (cab_label, offer_id)

        info = info_map.get(offer_id, {})
        ms = ms_map.get(offer_id, {})

        dcid = info.get("description_category_id")
        tid = info.get("type_id")

        category_name = category_map.get(int(dcid), "") if isinstance(dcid, int) else ""
        type_name = type_map.get(int(tid), "") if isinstance(tid, int) else ""

        ms_name = ms.get("name", "") if isinstance(ms, dict) else ""
        buy_price = money_from_ms((ms.get("buyPrice") or {}).get("value") if isinstance(ms, dict) else None)

        # --- ЦЕНЫ ---
        # 3 поля (old/min/your): если есть в таблице -> берём из таблицы, иначе берём из Ozon
        if key in existing_prices:
            old_price = existing_prices[key].get("old_price")
            min_price = existing_prices[key].get("min_price")
            your_price = existing_prices[key].get("your_price")
        else:
            p = prices_map_new.get(offer_id, {})
            old_price = money_from_ozon(p.get("old_price"))
            min_price = money_from_ozon(p.get("min_price"))
            your_price = money_from_ozon(p.get("marketing_seller_price"))

        # Цена для покупателя: всегда тянем актуальную
        # (берём из v5/product/info/prices для ВСЕХ offer_id — но чтобы не делать лишний вызов на каждый товар,
        #   можно позже оптимизировать, сейчас сделаем одним батчем отдельным запросом)
        # Быстро и правильно: сделаем общий prices_map_all один раз.
        # Здесь заглушка, реальное значение проставим ниже (см. main()).
        buyer_price = None

        row = {
            "cab": cab_label,
            "category": category_name,
            "type": type_name,
            "ms_name": ms_name,
            "offer_id": offer_id,  # без апострофа, его добавим при записи
            "buy_price": buy_price,
            "old_price": old_price,
            "min_price": min_price,
            "your_price": your_price,
            "buyer_price": buyer_price,
        }
        result.append(row)

    return result


def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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


def apply_buyer_prices(client_id: str, api_key: str, rows: List[Dict[str, Any]]) -> None:
    """Проставляет buyer_price (price для покупателя) из Ozon для всех offer_id."""
    offer_ids = [r.get("offer_id") for r in rows if r.get("offer_id")]
    offer_ids = [str(x) for x in offer_ids if x]
    prices_map_all = fetch_ozon_prices_by_offer_ids(client_id, api_key, offer_ids)
    for r in rows:
        oid = r.get("offer_id")
        if not oid:
            continue
        p = prices_map_all.get(str(oid), {})
        r["buyer_price"] = money_from_ozon(p.get("price"))


def main() -> None:
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("WORKSHEET_NAME", "API Ozon").strip()
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    ms_token = os.getenv("MS_TOKEN", "").strip()

    push_price = os.environ.get("PUSH_PRICE", "0").strip() in ("1", "true", "yes", "y")
    existing_prices = read_existing_prices(sheet, WORKSHEET_NAME)

    all_rows.extend(build_rows_for_cabinet("Cab1", cab1_id, cab1_key, ms_token, existing_prices, push_price))
    all_rows.extend(build_rows_for_cabinet("Cab2", cab2_id, cab2_key, ms_token, existing_prices, push_price))
 
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

    ws = connect_sheet(service_account_json, spreadsheet_id, worksheet_name)
    existing_keys, existing_prices = read_existing_sheet_prices(ws)

    all_rows: List[Dict[str, Any]] = []

    print("Sync Cab1...")
    cab1_rows = build_rows_for_cabinet("Cab1", cab1_id, cab1_key, ms_token, existing_keys, existing_prices)
    apply_buyer_prices(cab1_id, cab1_key, cab1_rows)
    all_rows.extend(cab1_rows)

    if cab2_id and cab2_key:
        print("Sync Cab2...")
        cab2_rows = build_rows_for_cabinet("Cab2", cab2_id, cab2_key, ms_token, existing_keys, existing_prices)
        apply_buyer_prices(cab2_id, cab2_key, cab2_rows)
        all_rows.extend(cab2_rows)

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
        # важно: апостроф, чтобы Google Sheets не “съел” ведущие нули
        offer_id_text = "'" + str(r.get("offer_id", "")).strip()

        sheet_rows.append(
            [
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
            ]
        )

    write_rows_to_sheet(ws, header, sheet_rows)
    print(f"Done. Written {len(sheet_rows)} rows to '{worksheet_name}'.")


if __name__ == "__main__":
    main()
