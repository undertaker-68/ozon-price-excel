#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py

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


def daterange_chunks(from_date: dt.date, to_date: dt.date, chunk_days: int = 30) -> Iterable[Tuple[dt.date, dt.date]]:
    cur = from_date
    while cur < to_date:
        nxt = min(cur + dt.timedelta(days=chunk_days), to_date)
        yield cur, nxt
        cur = nxt


# -------------------- product mapping (offer_id <-> sku) --------------------

def fetch_offer_to_sku(client_id: str, api_key: str, offer_ids: List[str]) -> Dict[str, int]:
    """
    /v3/product/info/list позволяет получить sku для offer_id.
    Делаем батчами, чтобы не упереться в лимиты.
    """
    offer_to_sku: Dict[str, int] = {}

    # нормализуем вход (и сохраняем исходные)
    uniq = []
    seen = set()
    for oid in offer_ids:
        o = (oid or "").strip()
        if not o:
            continue
        for k in {o, norm_offer_id(o)}:
            if k and k not in seen:
                uniq.append(k)
                seen.add(k)

    # батчи по 1000 (на всякий)
    BATCH = 1000
    for i in range(0, len(uniq), BATCH):
        batch = uniq[i:i + BATCH]
        data = ozon_post(
            client_id,
            api_key,
            "/v3/product/info/list",
            {
                "filter": {"offer_id": batch},
                "limit": len(batch),
            },
        )
        items = (data.get("result") or {}).get("items") or []
        for it in items:
            oid = str(it.get("offer_id") or "").strip()
            sku = to_int(it.get("sku"))
            if oid and sku:
                offer_to_sku[oid] = sku
                offer_to_sku[norm_offer_id(oid)] = sku

        time.sleep(0.15)

    return offer_to_sku


# -------------------- finance: transaction list --------------------

def is_customer_payment_operation(op: Dict[str, Any]) -> bool:
    """
    Пытаемся определить операции "Оплачено покупателем" по названию/типу.
    В разных ответах поле может называться по-разному.
    """
    name = str(
        op.get("operation_type_name")
        or op.get("type_name")
        or op.get("operation_name")
        or op.get("name")
        or ""
    ).lower()

    # рус/eng эвристики
    if ("покупател" in name and "оплат" in name):
        return True
    if ("customer" in name and "payment" in name):
        return True
    if ("payment from customer" in name) or ("customer paid" in name):
        return True

    # иногда “Оплата эквайринга” — не то; специально отсекаем
    if ("эквайр" in name) or ("acquiring" in name):
        return False

    return False


def iter_operation_items(op: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Вытаскиваем items из операции. Встречаются разные структуры:
    - op["posting"]["items"]
    - op["posting"]["products"]
    - op["items"]
    - op["products"]
    - op["services"] (нам не нужно)
    """
    posting = op.get("posting") or {}
    for key in ("items", "products"):
        arr = posting.get(key)
        if isinstance(arr, list):
            for x in arr:
                if isinstance(x, dict):
                    yield x

    for key in ("items", "products"):
        arr = op.get(key)
        if isinstance(arr, list):
            for x in arr:
                if isinstance(x, dict):
                    yield x


def get_item_sku(item: Dict[str, Any]) -> int:
    return to_int(item.get("sku") or item.get("product_id") or item.get("offer_sku") or item.get("id"))


def get_item_qty(item: Dict[str, Any]) -> int:
    q = to_int(item.get("quantity") or item.get("qty") or item.get("count"))
    return q if q > 0 else 1


def get_item_amount(item: Dict[str, Any]) -> float:
    # суммы могут лежать в amount/price/value/customer_price и т.п.
    for k in ("amount", "price", "value", "customer_price", "paid", "sum"):
        if k in item:
            return to_float(item.get(k))
    # иногда amount лежит глубже
    money = item.get("money") or {}
    if isinstance(money, dict):
        for k in ("amount", "value", "price"):
            if k in money:
                return to_float(money.get(k))
    return 0.0


def get_item_currency(item: Dict[str, Any], op: Dict[str, Any]) -> str:
    for k in ("currency_code", "currency"):
        v = item.get(k)
        if v:
            return str(v).upper()
    for k in ("currency_code", "currency"):
        v = op.get(k)
        if v:
            return str(v).upper()
    return "RUB"


def fetch_customer_payments(
    client_id: str,
    api_key: str,
    date_from: dt.date,
    date_to: dt.date,
) -> List[Dict[str, Any]]:
    """
    /v3/finance/transaction/list — максимум 30 дней, поэтому режем по чанкам.
    Возвращаем список операций.
    """
    all_ops: List[Dict[str, Any]] = []

    for frm, to in daterange_chunks(date_from, date_to, 30):
        page = 1
        while True:
            payload = {
                "filter": {
                    "date": {"from": iso_dt(frm), "to": iso_dt(to)},
                    "operation_type": [],          # все типы, отфильтруем сами
                    "posting_number": "",
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": 1000,
            }
            data = ozon_post(client_id, api_key, "/v3/finance/transaction/list", payload)
            result = data.get("result") or {}
            ops = result.get("operations") or result.get("operation") or result.get("transactions") or []
            if not isinstance(ops, list):
                ops = []

            all_ops.extend([o for o in ops if isinstance(o, dict)])

            # пагинация (варианты)
            has_next = result.get("has_next")
            if has_next is True:
                page += 1
                time.sleep(0.2)
                continue

            # если has_next нет — пробуем через total/page_count
            page_count = to_int(result.get("page_count"))
            if page_count and page < page_count:
                page += 1
                time.sleep(0.2)
                continue

            # fallback: если вернулось меньше page_size — конец
            if len(ops) == 1000:
                page += 1
                time.sleep(0.2)
                continue

            break

        time.sleep(0.2)

    return all_ops


def aggregate_paid_avg_by_offer(
    ops: List[Dict[str, Any]],
    sku_to_offer: Dict[int, str],
) -> Dict[str, Tuple[int, float]]:
    """
    Возвращает: offer_id -> (qty_total, sum_paid_total)
    """
    qty = defaultdict(int)
    summ = defaultdict(float)

    for op in ops:
        if not is_customer_payment_operation(op):
            continue

        for it in iter_operation_items(op):
            cur = get_item_currency(it, op)
            if not is_rub(cur):
                continue

            sku = get_item_sku(it)
            if not sku:
                continue

            offer_id = sku_to_offer.get(sku)
            if not offer_id:
                continue

            q = get_item_qty(it)
            a = get_item_amount(it)

            # некоторые ответы дают сумму за всю строку, некоторые — за штуку.
            # UI “Оплачено покупателем” обычно за строку (qty=1 чаще всего),
            # поэтому считаем: SUM(amount) и SUM(qty) => avg = sum/qty.
            qty[offer_id] += q
            summ[offer_id] += a

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

    # headers
    for cell, val in HEADERS.items():
        ws.update(range_name=cell, values=[[val]])

    offer_ids = ws.col_values(4)[START_ROW - 1 :]
    offer_ids = [(x or "").strip() for x in offer_ids]

    # -------- build sku maps for both accounts, then merge (на случай, если товары пересекаются)
    offer_to_sku_1 = fetch_offer_to_sku(oz1_id, oz1_key, offer_ids)
    offer_to_sku_2 = fetch_offer_to_sku(oz2_id, oz2_key, offer_ids)

    sku_to_offer: Dict[int, str] = {}
    for oid, sku in offer_to_sku_1.items():
        if sku:
            sku_to_offer[sku] = oid
    for oid, sku in offer_to_sku_2.items():
        if sku and sku not in sku_to_offer:
            sku_to_offer[sku] = oid

    # -------- pull finance ops (customer payments) for both accounts
    ops_90_1 = fetch_customer_payments(oz1_id, oz1_key, date_from_90, date_to)
    ops_90_2 = fetch_customer_payments(oz2_id, oz2_key, date_from_90, date_to)
    agg90_1 = aggregate_paid_avg_by_offer(ops_90_1, sku_to_offer)
    agg90_2 = aggregate_paid_avg_by_offer(ops_90_2, sku_to_offer)

    ops_7_1 = fetch_customer_payments(oz1_id, oz1_key, date_from_7, date_to)
    ops_7_2 = fetch_customer_payments(oz2_id, oz2_key, date_from_7, date_to)
    agg7_1 = aggregate_paid_avg_by_offer(ops_7_1, sku_to_offer)
    agg7_2 = aggregate_paid_avg_by_offer(ops_7_2, sku_to_offer)

    # -------- merge account totals
    Q90 = defaultdict(int)
    S90 = defaultdict(float)
    Q7 = defaultdict(int)
    S7 = defaultdict(float)

    def merge_into(src: Dict[str, Tuple[int, float]], Q: Dict[str, int], S: Dict[str, float]) -> None:
        for oid, (q, s) in src.items():
            if not oid:
                continue
            Q[oid] += int(q)
            S[oid] += float(s)

    merge_into(agg90_1, Q90, S90)
    merge_into(agg90_2, Q90, S90)
    merge_into(agg7_1, Q7, S7)
    merge_into(agg7_2, Q7, S7)

    # -------- write E–H: qty + avg_paid
    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", ""])
            continue

        key = oid if oid in Q90 else norm_offer_id(oid)

        q90 = Q90.get(key, 0)
        s90 = S90.get(key, 0.0)
        q7 = Q7.get(key, 0)
        s7 = S7.get(key, 0.0)

        avg90 = round(s90 / q90, 2) if q90 else ""
        avg7 = round(s7 / q7, 2) if q7 else (avg90 if avg90 != "" else "")

        rows.append([q90, avg90, q7, avg7])

    ws.update(
        range_name=f"E{START_ROW}:H{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    print("OK: finance-based E–H updated (like Ozon UI)")


if __name__ == "__main__":
    main()
