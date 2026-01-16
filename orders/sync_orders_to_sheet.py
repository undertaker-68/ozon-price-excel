#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py
#
# "Как в Ozon" для налога:
# - Берем финансовые операции из /v3/finance/transaction/list (type == "orders")
# - Используем accruals_for_sale как базу "оплачено покупателем" на уровне заказа
# - Распределяем accruals_for_sale по товарам внутри операции пропорционально quantity
# - Аггрегируем по SKU (из items[].sku)
#
# В Google Sheets:
# - Лист "Заказы Ozon": D = offer_id; E..H будут обновлены скриптом
# - Лист "API Ozon": содержит SKU (по словам пользователя — колонка D) и offer_id (часто колонка F).
#   Скрипт пытается найти колонки по заголовкам, иначе использует fallback: sku_col=4 (D), offer_col=6 (F)
#
# Запись в лист "Заказы Ozon":
# E = кол-во (90 дней)
# F = оплачено покупателем (среднее, 90 дней)
# G = кол-во (7 дней)
# H = оплачено покупателем (среднее, 7 дней)

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


SHEET_ORDERS = "Заказы Ozon"
SHEET_API = "API Ozon"
START_ROW = 2
OZON_API_BASE = "https://api-seller.ozon.ru"

# Если хочешь, можно отключить перезапись заголовков
WRITE_HEADERS = True

HEADERS = {
    "A1": "Категория",
    "B1": "Тип",
    "C1": "Название",
    "D1": "offer_id",
    "E1": "Кол-во (90 дней)",
    "F1": "Оплачено покупателем (среднее, 90 дней)",
    "G1": "Кол-во (7 дней)",
    "H1": "Оплачено покупателем (среднее, 7 дней)",
}


def iso_dt(d: dt.date) -> str:
    return d.strftime("%Y-%m-%dT00:00:00Z")


def to_float(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def to_int(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, int):
        return x
    try:
        return int(float(str(x).strip().replace(",", ".")))
    except Exception:
        return 0


def norm_offer_id(oid: str) -> str:
    s = (oid or "").strip()
    if s.isdigit():
        try:
            return str(int(s))
        except Exception:
            return s
    return s


def is_rub(code: Any) -> bool:
    if code is None or str(code).strip() == "":
        return True  # правило A
    return str(code).upper() == "RUB"


def ozon_post(client_id: str, api_key: str, path: str, payload: dict) -> dict:
    url = OZON_API_BASE + path
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    if not r.ok:
        raise Exception(f"Ozon {path} {r.status_code}: {r.text}")
    return r.json()


def daterange_chunks(date_from: dt.date, date_to: dt.date, chunk_days: int = 30) -> Iterable[Tuple[dt.date, dt.date]]:
    cur = date_from
    while cur < date_to:
        nxt = min(cur + dt.timedelta(days=chunk_days), date_to)
        yield cur, nxt
        cur = nxt


# ---------------- finance transaction list ----------------

def iter_operation_items(op: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    arr = op.get("items")
    if isinstance(arr, list):
        for x in arr:
            if isinstance(x, dict):
                yield x

    posting = op.get("posting") or {}
    arr2 = posting.get("items")
    if isinstance(arr2, list):
        for x in arr2:
            if isinstance(x, dict):
                yield x


def get_item_sku(item: Dict[str, Any]) -> int:
    return to_int(item.get("sku") or item.get("product_id") or item.get("id"))


def get_item_qty(item: Dict[str, Any]) -> int:
    q = to_int(item.get("quantity") or item.get("qty") or item.get("count"))
    return q if q > 0 else 1


def fetch_transactions(client_id: str, api_key: str, date_from: dt.date, date_to: dt.date) -> List[Dict[str, Any]]:
    all_ops: List[Dict[str, Any]] = []

    for frm, to in daterange_chunks(date_from, date_to, 30):
        page = 1
        while True:
            payload = {
                "filter": {
                    "date": {"from": iso_dt(frm), "to": iso_dt(to)},
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": 1000,
            }
            data = ozon_post(client_id, api_key, "/v3/finance/transaction/list", payload)
            result = data.get("result") or {}
            ops = result.get("operations") or []
            if not isinstance(ops, list):
                ops = []

            all_ops.extend([o for o in ops if isinstance(o, dict)])

            if result.get("has_next") is True:
                page += 1
                time.sleep(0.2)
                continue

            page_count = to_int(result.get("page_count"))
            if page_count and page < page_count:
                page += 1
                time.sleep(0.2)
                continue

            if len(ops) == 1000:
                page += 1
                time.sleep(0.2)
                continue

            break

        time.sleep(0.2)

    return all_ops


def collect_orders_ops(ops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for op in ops:
        if str(op.get("type") or "").lower() != "orders":
            continue
        accr = to_float(op.get("accruals_for_sale"))
        if accr <= 0:
            continue
        cur = op.get("currency_code") or op.get("currency")
        if not is_rub(cur):
            continue
        out.append(op)
    return out


def aggregate_paid_by_sku(ops: List[Dict[str, Any]]) -> Dict[int, Tuple[int, float]]:
    """sku -> (qty_total, sum_paid_total)"""
    qty = defaultdict(int)
    summ = defaultdict(float)

    for op in ops:
        accr = to_float(op.get("accruals_for_sale"))
        if accr <= 0:
            continue

        items = list(iter_operation_items(op))
        if not items:
            continue

        total_q = sum(get_item_qty(it) for it in items) or 1

        for it in items:
            sku = get_item_sku(it)
            if not sku:
                continue
            q = get_item_qty(it)
            part = accr * (q / total_q)
            qty[sku] += q
            summ[sku] += part

    return {k: (qty[k], summ[k]) for k in (set(qty.keys()) | set(summ.keys()))}


# ---------------- Google Sheets mapping offer_id -> sku ----------------

def find_col_by_header(headers: List[str], needles: List[str]) -> Optional[int]:
    low = [str(x or "").strip().lower() for x in headers]
    for n in needles:
        n = n.lower()
        for idx, h in enumerate(low, start=1):
            if n == h or n in h:
                return idx
    return None


def build_offer_to_sku(ws_api) -> Dict[str, int]:
    header_row = ws_api.row_values(1)
    sku_col = find_col_by_header(header_row, ["sku"])
    offer_col = find_col_by_header(header_row, ["offer_id", "offer id", "артикул", "offer"])

    # Fallback по твоим словам/предыдущим формулам:
    if sku_col is None:
        sku_col = 4  # D
    if offer_col is None:
        offer_col = 6  # F

    sku_vals = ws_api.col_values(sku_col)[1:]  # начиная со 2 строки
    offer_vals = ws_api.col_values(offer_col)[1:]

    out: Dict[str, int] = {}
    for off, sku in zip(offer_vals, sku_vals):
        off_s = str(off or "").strip()
        sku_i = to_int(sku)
        if not off_s or not sku_i:
            continue
        out[off_s] = sku_i
        out[norm_offer_id(off_s)] = sku_i

    return out


def main() -> None:
    oz1_id = os.environ["OZON1_CLIENT_ID"]
    oz1_key = os.environ["OZON1_API_KEY"]
    oz2_id = os.environ["OZON2_CLIENT_ID"]
    oz2_key = os.environ["OZON2_API_KEY"]

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_json = os.environ["GOOGLE_CREDS_JSON"]

    today = dt.date.today()
    date_to = today + dt.timedelta(days=1)
    date_from_90 = today - dt.timedelta(days=90)
    date_from_7 = today - dt.timedelta(days=7)

    gc = gspread.authorize(
        Credentials.from_service_account_file(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    )
    sh = gc.open_by_key(sheet_id)
    ws_orders = sh.worksheet(SHEET_ORDERS)
    ws_api = sh.worksheet(SHEET_API)

    if WRITE_HEADERS:
        for cell, val in HEADERS.items():
            ws_orders.update(range_name=cell, values=[[val]])

    # offer_id из листа Заказы Ozon (колонка D)
    offer_ids = ws_orders.col_values(4)[START_ROW - 1 :]
    offer_ids = [(x or "").strip() for x in offer_ids]

    offer_to_sku = build_offer_to_sku(ws_api)

    # Финансы по 2 кабинетам
    ops90 = collect_orders_ops(
        fetch_transactions(oz1_id, oz1_key, date_from_90, date_to)
        + fetch_transactions(oz2_id, oz2_key, date_from_90, date_to)
    )
    ops7 = collect_orders_ops(
        fetch_transactions(oz1_id, oz1_key, date_from_7, date_to)
        + fetch_transactions(oz2_id, oz2_key, date_from_7, date_to)
    )

    agg90 = aggregate_paid_by_sku(ops90)
    agg7 = aggregate_paid_by_sku(ops7)

    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", ""])
            continue

        sku = offer_to_sku.get(oid) or offer_to_sku.get(norm_offer_id(oid))
        if not sku:
            rows.append([0, "", 0, ""])  # нет SKU — нечего считать
            continue

        q90, s90 = agg90.get(sku, (0, 0.0))
        q7, s7 = agg7.get(sku, (0, 0.0))

        avg90 = round(s90 / q90, 2) if q90 else ""
        avg7 = round(s7 / q7, 2) if q7 else (avg90 if avg90 != "" else "")

        rows.append([q90, avg90, q7, avg7])

    ws_orders.update(
        range_name=f"E{START_ROW}:H{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    print("OK: finance-based E–H updated (by SKU via 'API Ozon' mapping)")


if __name__ == "__main__":
    main()
