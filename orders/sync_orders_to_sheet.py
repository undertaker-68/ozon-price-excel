#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py
#
# Цель: как в UI Ozon ("Оплачено покупателем") — берём из finance transaction list
# и считаем среднюю цену на сервере: SUM(accruals_for_sale)/SUM(qty)
#
# В лист пишем:
# E = qty90
# F = avg_paid90
# G = qty7
# H = avg_paid7

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


# -------------------- helpers --------------------

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


# -------------------- product mapping (offer_id -> sku) --------------------

def _clean_offer_ids(offer_ids: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for oid in offer_ids:
        s = (oid or "").strip()
        if not s:
            continue
        for k in (s, norm_offer_id(s)):
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    return out


def fetch_offer_to_sku(client_id: str, api_key: str, offer_ids: List[str]) -> Dict[str, int]:
    """
    Ozon /v3/product/info/list иногда принимает:
      A) {"offer_id":[...], "limit":1000}
      B) {"filter":{"offer_id":[...]}, "limit":1000}
    Поэтому делаем fallback.
    Плюс: не отправляем пустые батчи (иначе ошибка "use either offer_id or product_id or sku").
    """
    offer_to_sku: Dict[str, int] = {}
    uniq = _clean_offer_ids(offer_ids)
    if not uniq:
        return offer_to_sku

    BATCH = 1000
    for i in range(0, len(uniq), BATCH):
        batch = uniq[i:i + BATCH]
        if not batch:
            continue

        # пробуем формат A, если не выйдет — B
        tried = []
        for payload in (
            {"offer_id": batch, "limit": 1000},
            {"filter": {"offer_id": batch}, "limit": 1000},
        ):
            try:
                data = ozon_post(client_id, api_key, "/v3/product/info/list", payload)
                result = data.get("result") or {}
                items = result.get("items") or []
                for it in items:
                    oid = str(it.get("offer_id") or "").strip()
                    sku = to_int(it.get("sku"))
                    if oid and sku:
                        offer_to_sku[oid] = sku
                        offer_to_sku[norm_offer_id(oid)] = sku
                break
            except Exception as e:
                tried.append(str(e))
                # если это не "use either ..." — тоже попробуем fallback
                continue

        time.sleep(0.15)

    return offer_to_sku


# -------------------- finance: transaction list --------------------

def iter_operation_items(op: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # В твоём логе items лежат на верхнем уровне: op["items"] (см. :contentReference[oaicite:1]{index=1})
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


def fetch_transactions(
    client_id: str,
    api_key: str,
    date_from: dt.date,
    date_to: dt.date,
) -> List[Dict[str, Any]]:
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

            has_next = result.get("has_next")
            if has_next is True:
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


def aggregate_paid_from_orders_ops(
    ops: List[Dict[str, Any]],
    sku_to_offer: Dict[int, str],
) -> Dict[str, Tuple[int, float]]:
    """
    Берём операции "orders" (в твоём логе это type:"orders", operation_type_name:"Доставка покупателю")
    и используем accruals_for_sale как "деньги от покупателя" на уровне posting.
    Далее распределяем на items (обычно 1 item, но делаем пропорцию по qty).
    """
    qty = defaultdict(int)
    summ = defaultdict(float)

    for op in ops:
        # строго отсекаем лишнее (эквайринг/услуги/возвраты)
        if str(op.get("type") or "").lower() != "orders":
            continue

        accr = to_float(op.get("accruals_for_sale"))
        if accr <= 0:
            continue

        # валюта в transaction/list часто не приходит — считаем RUB (правило A)
        # если вдруг появится поле currency_code — учтём
        cur = op.get("currency_code") or op.get("currency")
        if not is_rub(cur):
            continue

        items = list(iter_operation_items(op))
        if not items:
            continue

        # распределяем по qty
        total_q = sum(get_item_qty(it) for it in items) or 1

        for it in items:
            sku = get_item_sku(it)
            if not sku:
                continue
            offer_id = sku_to_offer.get(sku)
            if not offer_id:
                continue

            q = get_item_qty(it)
            part = accr * (q / total_q)

            qty[offer_id] += q
            summ[offer_id] += part

    out: Dict[str, Tuple[int, float]] = {}
    for oid in set(list(qty.keys()) + list(summ.keys())):
        out[oid] = (qty[oid], summ[oid])
        out[norm_offer_id(oid)] = (qty[oid], summ[oid])
    return out


# -------------------- main --------------------

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

    # offer_id -> sku (по двум аккаунтам) + обратный индекс sku -> offer_id
    offer_to_sku = {}
    offer_to_sku.update(fetch_offer_to_sku(oz1_id, oz1_key, offer_ids))
    offer_to_sku.update(fetch_offer_to_sku(oz2_id, oz2_key, offer_ids))

    if not offer_to_sku:
        raise Exception("Не удалось построить offer_id -> sku. Проверь, что в колонке D есть offer_id и они существуют в Ozon.")

    sku_to_offer: Dict[int, str] = {}
    for oid, sku in offer_to_sku.items():
        if sku:
            # сохраняем первый попавшийся offer_id как “каноничный”
            sku_to_offer.setdefault(sku, oid)

    # тянем транзакции и агрегируем
    ops90_1 = fetch_transactions(oz1_id, oz1_key, date_from_90, date_to)
    ops90_2 = fetch_transactions(oz2_id, oz2_key, date_from_90, date_to)
    agg90 = aggregate_paid_from_orders_ops(ops90_1 + ops90_2, sku_to_offer)

    ops7_1 = fetch_transactions(oz1_id, oz1_key, date_from_7, date_to)
    ops7_2 = fetch_transactions(oz2_id, oz2_key, date_from_7, date_to)
    agg7 = aggregate_paid_from_orders_ops(ops7_1 + ops7_2, sku_to_offer)

    # пишем E–H: qty + avg_paid
    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", ""])
            continue

        k = oid if oid in agg90 else norm_offer_id(oid)
        q90, s90 = agg90.get(k, (0, 0.0))
        q7, s7 = agg7.get(k, (0, 0.0))

        avg90 = round(s90 / q90, 2) if q90 else ""
        avg7 = round(s7 / q7, 2) if q7 else (avg90 if avg90 != "" else "")

        rows.append([q90, avg90, q7, avg7])

    ws.update(
        range_name=f"E{START_ROW}:H{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    print("OK: finance-based E–H updated")


if __name__ == "__main__":
    main()
