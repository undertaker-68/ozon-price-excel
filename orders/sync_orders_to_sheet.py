#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py
#
# "Как в Ozon (страница Заказы)": берём оплату из posting.financial_data.products[].customer_price (RUB)
# Источники:
#   - FBS: /v3/posting/fbs/list
#   - FBO: /v2/posting/fbo/list
#
# В Google Sheet "Заказы Ozon" пишем только:
#   E qty90
#   F avg_paid90
#   G qty7
#   H avg_paid7
#
# Маппинг offer_id -> sku берём из листа "API Ozon" (таблица товаров).

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


OZON_API_BASE = "https://api-seller.ozon.ru"

SHEET_ORDERS = "Заказы Ozon"
SHEET_API = "API Ozon"
START_ROW = 2  # данные начинаются со 2 строки (шапка в 1-й)

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


# ---------- utils ----------

def iso_dt(d: dt.datetime) -> str:
    # Ozon ожидает ISO8601 с Z
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, int):
            return x
        return int(float(str(x).strip().replace(",", ".")))
    except Exception:
        return 0


def to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x).strip().replace(",", "."))
    except Exception:
        return 0.0


def is_rub(code: Any) -> bool:
    # правило A: если валюты нет -> считаем RUB
    if code is None or str(code).strip() == "":
        return True
    return str(code).upper() == "RUB"


def ozon_post(client_id: str, api_key: str, path: str, payload: dict) -> dict:
    url = OZON_API_BASE + path
    headers = {
        "Client-Id": str(client_id),
        "Api-Key": str(api_key),
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    if not r.ok:
        raise Exception(f"Ozon {path} {r.status_code}: {r.text}")
    return r.json()


# ---------- Google Sheets mapping offer_id -> sku from "API Ozon" ----------

def build_offer_to_sku(ws_api) -> Dict[str, int]:
    """
    Пытаемся найти колонки offer_id и sku по заголовкам.
    Если не нашли — используем fallback:
      offer_id = колонка D (4)
      sku      = колонка Q (17)  <-- если не так, скажи и поменяем
    """
    values = ws_api.get_all_values()
    if not values:
        raise Exception("Лист 'API Ozon' пустой")

    header = values[0]
    idx_offer = None
    idx_sku = None

    def norm(s: str) -> str:
        return (s or "").strip().lower()

    for i, name in enumerate(header):
        n = norm(name)
        if n in ("offer_id", "offerid", "offer id", "артикул продавца", "артикул"):
            idx_offer = i
        if n in ("sku", "ozon_sku", "ozon sku"):
            idx_sku = i

    # fallback (если заголовков нет или они другие)
    if idx_offer is None:
        idx_offer = 3  # D
    if idx_sku is None:
        idx_sku = 16   # Q (часто так). Если у тебя иначе — скажи букву.

    offer_to_sku: Dict[str, int] = {}
    for row in values[1:]:
        if idx_offer >= len(row) or idx_sku >= len(row):
            continue
        offer = (row[idx_offer] or "").strip()
        sku = to_int(row[idx_sku])
        if offer and sku:
            offer_to_sku[offer] = sku
            # нормализация "00512" -> "512" тоже иногда нужна
            if offer.isdigit():
                offer_to_sku[str(int(offer))] = sku

    return offer_to_sku


# ---------- Ozon postings fetchers ----------

def fetch_fbs_postings(client_id: str, api_key: str, since: str, to: str) -> List[dict]:
    postings: List[dict] = []
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "desc",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        }
        data = ozon_post(client_id, api_key, "/v3/posting/fbs/list", payload)
        res = data.get("result") or {}
        batch = res.get("postings") or []
        postings.extend(batch)

        has_next = res.get("has_next")
        if has_next is True:
            offset += limit
            time.sleep(0.2)
            continue

        # иногда has_next нет — ориентируемся по длине
        if len(batch) == limit:
            offset += limit
            time.sleep(0.2)
            continue

        break

    return postings


def fetch_fbo_postings(client_id: str, api_key: str, since: str, to: str) -> List[dict]:
    postings: List[dict] = []
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "desc",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        }
        data = ozon_post(client_id, api_key, "/v2/posting/fbo/list", payload)
        batch = data.get("result") or []
        postings.extend(batch)

        # у FBO list обычно нет has_next — идём по offset пока не станет меньше limit
        if len(batch) == limit:
            offset += limit
            time.sleep(0.2)
            continue
        break

    return postings


def iter_paid_lines(posting: dict) -> List[Tuple[str, int, float]]:
    """
    Возвращает список линий (offer_id, qty, customer_price) для posting.
    Берём строго из financial_data.products[]:
      customer_price + customer_currency_code == RUB
    """
    lines: List[Tuple[str, int, float]] = []

    fin = posting.get("financial_data") or {}
    fin_products = fin.get("products") or []
    if not isinstance(fin_products, list):
        return lines

    # offer_id берём из posting.products по product_id/sku соответствию как fallback
    # но в большинстве случаев проще: в fin_products есть product_id, а offer_id берём из posting.products
    posting_products = posting.get("products") or []
    prodid_to_offer: Dict[int, str] = {}
    for p in posting_products:
        pid = to_int(p.get("sku") or p.get("product_id") or p.get("id"))
        oid = str(p.get("offer_id") or "").strip()
        if pid and oid:
            prodid_to_offer[pid] = oid

    for pr in fin_products:
        cur = pr.get("customer_currency_code") or pr.get("currency_code") or "RUB"
        if not is_rub(cur):
            continue
        customer_price = to_float(pr.get("customer_price"))
        if customer_price <= 0:
            continue
        qty = to_int(pr.get("quantity")) or 1

        pid = to_int(pr.get("product_id") or pr.get("sku"))
        offer_id = str(pr.get("offer_id") or "").strip()  # иногда бывает
        if not offer_id and pid:
            offer_id = prodid_to_offer.get(pid, "")

        if offer_id:
            lines.append((offer_id, qty, customer_price))

    return lines


def aggregate_paid(postings: List[dict]) -> Dict[str, Tuple[int, float]]:
    """
    offer_id -> (qty_sum, paid_sum)
    """
    qty = defaultdict(int)
    paid = defaultdict(float)

    for p in postings:
        for offer_id, q, customer_price in iter_paid_lines(p):
            qty[offer_id] += q
            paid[offer_id] += customer_price

    out: Dict[str, Tuple[int, float]] = {}
    for oid in set(list(qty.keys()) + list(paid.keys())):
        out[oid] = (qty[oid], paid[oid])
        if oid.isdigit():
            out[str(int(oid))] = (qty[oid], paid[oid])
    return out


# ---------- main ----------

def main() -> None:
    # env names from your .env
    oz1_id = os.environ["OZON_CLIENT_ID_1"]
    oz1_key = os.environ["OZON_API_KEY_1"]
    oz2_id = os.environ.get("OZON_CLIENT_ID_2", "")
    oz2_key = os.environ.get("OZON_API_KEY_2", "")

    sheet_id = os.environ["SPREADSHEET_ID"]
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    gc = gspread.authorize(
        Credentials.from_service_account_file(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    )
    sh = gc.open_by_key(sheet_id)
    ws_orders = sh.worksheet(SHEET_ORDERS)
    ws_api = sh.worksheet(SHEET_API)

    # headers (только чтобы названия были норм)
    for cell, val in HEADERS.items():
        ws_orders.update(range_name=cell, values=[[val]])

    # offer_id list from Orders sheet col D
    offer_ids = ws_orders.col_values(4)[START_ROW - 1 :]
    offer_ids = [(x or "").strip() for x in offer_ids]

    # mapping offer_id -> sku from API sheet (на будущее, если нужно)
    offer_to_sku = build_offer_to_sku(ws_api)
    if not offer_to_sku:
        print("WARN: offer_to_sku mapping empty (API Ozon sheet). Continue anyway.")

    now = dt.datetime.utcnow()
    since_7 = iso_dt(now - dt.timedelta(days=7))
    since_90 = iso_dt(now - dt.timedelta(days=90))
    to = iso_dt(now + dt.timedelta(days=1))

    # fetch postings for 90 days
    p90: List[dict] = []
    p90 += fetch_fbs_postings(oz1_id, oz1_key, since_90, to)
    p90 += fetch_fbo_postings(oz1_id, oz1_key, since_90, to)
    if oz2_id and oz2_key:
        p90 += fetch_fbs_postings(oz2_id, oz2_key, since_90, to)
        p90 += fetch_fbo_postings(oz2_id, oz2_key, since_90, to)

    agg90 = aggregate_paid(p90)

    # fetch postings for 7 days
    p7: List[dict] = []
    p7 += fetch_fbs_postings(oz1_id, oz1_key, since_7, to)
    p7 += fetch_fbo_postings(oz1_id, oz1_key, since_7, to)
    if oz2_id and oz2_key:
        p7 += fetch_fbs_postings(oz2_id, oz2_key, since_7, to)
        p7 += fetch_fbo_postings(oz2_id, oz2_key, since_7, to)

    agg7 = aggregate_paid(p7)

    # prepare rows E–H
    rows: List[List[Any]] = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", ""])
            continue

        k = oid if oid in agg90 else (str(int(oid)) if oid.isdigit() else oid)
        q90, s90 = agg90.get(k, (0, 0.0))
        avg90 = round(s90 / q90, 2) if q90 else ""

        k7 = oid if oid in agg7 else (str(int(oid)) if oid.isdigit() else oid)
        q7, s7 = agg7.get(k7, (0, 0.0))
        avg7 = round(s7 / q7, 2) if q7 else ""

        rows.append([q90, avg90, q7, avg7])

    ws_orders.update(
        range_name=f"E{START_ROW}:H{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    print("OK: postings-based E–H updated (customer_price)")


if __name__ == "__main__":
    main()
