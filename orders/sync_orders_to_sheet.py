#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py
#
# "Как в Ozon": считаем среднее "Оплачено покупателем" из finance transaction list.
# Больше НЕ строим offer_id->sku по offer_id из таблицы (это ломалось).
# Теперь: берём sku из транзакций -> по sku получаем offer_id -> агрегируем.

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


SHEET_NAME = "Заказы Ozon"
START_ROW = 2
OZON_API_BASE = "https://api-seller.ozon.ru"

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
    # правило A: если валюты нет -> считаем RUB
    if code is None or str(code).strip() == "":
        return True
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


def daterange_chunks(from_date: dt.date, to_date: dt.date, chunk_days: int = 30) -> Iterable[Tuple[dt.date, dt.date]]:
    cur = from_date
    while cur < to_date:
        nxt = min(cur + dt.timedelta(days=chunk_days), to_date)
        yield cur, nxt
        cur = nxt


# -------- finance transaction list --------

def iter_operation_items(op: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # В реальных ответах у тебя items есть на верхнем уровне
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
    # Берём только type=="orders" и accruals_for_sale>0
    out = []
    for op in ops:
        if str(op.get("type") or "").lower() != "orders":
            continue
        if to_float(op.get("accruals_for_sale")) <= 0:
            continue
        cur = op.get("currency_code") or op.get("currency")  # часто пусто
        if not is_rub(cur):
            continue
        out.append(op)
    return out

# --- гарантируем формулу SKU в колонке C ---
for i in range(START_ROW, START_ROW + len(ws.col_values(5))):
    ws.update(
        f"C{i}",
        f'=IFERROR(VLOOKUP(E{i};\'API Ozon\'!A:B;2;0);"")'
    )


def extract_skus_from_ops(ops: List[Dict[str, Any]]) -> List[int]:
    skus = set()
    for op in ops:
        for it in iter_operation_items(op):
            sku = get_item_sku(it)
            if sku:
                skus.add(sku)
    return sorted(skus)


# -------- product info list: sku -> offer_id --------

def fetch_sku_to_offer(client_id: str, api_key: str, skus: List[int]) -> Dict[int, str]:
    """
    Правильный формат /v3/product/info/list: верхний уровень sku/product_id/offer_id массивами
    (без filter). :contentReference[oaicite:1]{index=1}
    """
    sku_to_offer: Dict[int, str] = {}
    BATCH = 1000
    for i in range(0, len(skus), BATCH):
        batch = skus[i:i + BATCH]
        if not batch:
            continue
        data = ozon_post(client_id, api_key, "/v3/product/info/list", {"sku": batch})
        items = (data.get("result") or {}).get("items") or []
        for it in items:
            sku = to_int(it.get("sku"))
            oid = str(it.get("offer_id") or "").strip()
            if sku and oid:
                sku_to_offer[sku] = oid
        time.sleep(0.15)
    return sku_to_offer


def aggregate_avg_paid(ops: List[Dict[str, Any]], sku_to_offer: Dict[int, str]) -> Dict[str, Tuple[int, float]]:
    """
    На уровне операции берём accruals_for_sale и распределяем по items пропорционально qty.
    Возвращаем offer_id -> (qty_total, sum_paid_total)
    """
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
            oid = sku_to_offer.get(sku)
            if not oid:
                continue
            q = get_item_qty(it)
            part = accr * (q / total_q)

            qty[oid] += q
            summ[oid] += part

    out: Dict[str, Tuple[int, float]] = {}
    for oid in set(list(qty.keys()) + list(summ.keys())):
        out[oid] = (qty[oid], summ[oid])
        out[norm_offer_id(oid)] = (qty[oid], summ[oid])
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
    ws = gc.open_by_key(sheet_id).worksheet(SHEET_NAME)

    for cell, val in HEADERS.items():
        ws.update(range_name=cell, values=[[val]])

    offer_ids = ws.col_values(4)[START_ROW - 1 :]
    offer_ids = [(x or "").strip() for x in offer_ids]

    # 1) Берём транзакции (90/7) по двум аккаунтам
    ops90 = fetch_transactions(oz1_id, oz1_key, date_from_90, date_to) + fetch_transactions(oz2_id, oz2_key, date_from_90, date_to)
    ops7 = fetch_transactions(oz1_id, oz1_key, date_from_7, date_to) + fetch_transactions(oz2_id, oz2_key, date_from_7, date_to)

    ops90 = collect_orders_ops(ops90)
    ops7 = collect_orders_ops(ops7)

    # 2) Собираем sku из транзакций и строим sku->offer_id
    skus = sorted(set(extract_skus_from_ops(ops90) + extract_skus_from_ops(ops7)))
    if not skus:
        raise Exception("В finance/transaction/list не нашёлся ни один sku в items[]. Проверь, что там реально есть операции type='orders' и items[].")

    sku_to_offer = {}
    sku_to_offer.update(fetch_sku_to_offer(oz1_id, oz1_key, skus))
    sku_to_offer.update(fetch_sku_to_offer(oz2_id, oz2_key, skus))
    if not sku_to_offer:
        raise Exception("Не удалось построить sku -> offer_id через /v3/product/info/list. Проверь доступы и что sku существуют.")

    # 3) Агрегируем (qty, sum_paid) и считаем среднее на сервере
    agg90 = aggregate_avg_paid(ops90, sku_to_offer)
    agg7 = aggregate_avg_paid(ops7, sku_to_offer)

    # 4) Пишем E–H: qty + avg_paid
    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", ""])
            continue

        k90 = oid if oid in agg90 else norm_offer_id(oid)
        q90, s90 = agg90.get(k90, (0, 0.0))

        k7 = oid if oid in agg7 else norm_offer_id(oid)
        q7, s7 = agg7.get(k7, (0, 0.0))

        avg90 = round(s90 / q90, 2) if q90 else ""
        avg7 = round(s7 / q7, 2) if q7 else (avg90 if avg90 != "" else "")

        rows.append([q90, avg90, q7, avg7])

    ws.update(
        range_name=f"E{START_ROW}:H{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    print("OK: finance-based E–H updated (sku->offer_id from transactions)")


if __name__ == "__main__":
    main()
