#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ================= НАСТРОЙКИ =================

SHEET_NAME = "Заказы Ozon"
START_ROW = 2

HEADERS = {
    "A1": "Категория",
    "B1": "Тип",
    "C1": "Название",
    "D1": "offer_id",
    "E1": "Заказы 90 дней, шт",
    "F1": "Средняя цена 90 дней",
    "G1": "Заказы 7 дней, шт",
    "H1": "Средняя цена для покупателя",
    "I1": "Итого получено от Ozon (7 дней)",
    "J1": "Чистая прибыль (7 дней)",
}

OZON_API_BASE = "https://api-seller.ozon.ru"


# ================= УТИЛИТЫ =================

def iso_dt(d: dt.date) -> str:
    return d.strftime("%Y-%m-%dT00:00:00Z")


def ozon_post(client_id: str, api_key: str, path: str, payload: dict) -> dict:
    url = OZON_API_BASE + path
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    if not r.ok:
        raise Exception(f"Ozon {r.status_code}: {r.text}")
    return r.json()


def _parse_iso_date(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def get_post_date(posting: Dict[str, Any]) -> Optional[dt.date]:
    for key in (
        "created_at",
        "in_process_at",
        "processed_at",
        "shipment_date",
        "shipped_at",
        "delivering_date",
    ):
        d = _parse_iso_date(posting.get(key))
        if d:
            return d
    return None


def is_rub_product(product_row: Dict[str, Any], posting: Dict[str, Any]) -> bool:
    code = (
        product_row.get("currency_code")
        or (posting.get("financial_data") or {}).get("currency_code")
        or posting.get("currency_code")
    )
    if not code:
        return True
    return str(code).upper() == "RUB"


# ================= OZON =================

def fetch_fbs(client_id: str, api_key: str, since: str, to: str):
    offset = 0
    limit = 1000
    while True:
        data = ozon_post(
            client_id,
            api_key,
            "/v3/posting/fbs/list",
            {
                "dir": "asc",
                "filter": {"since": since, "to": to},
                "limit": limit,
                "offset": offset,
                "with": {"financial_data": True, "products": True},
            },
        )
        postings = ((data.get("result") or {}).get("postings")) or []
        for p in postings:
            yield p
        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.2)


def fetch_fbo(client_id: str, api_key: str, since: str, to: str):
    offset = 0
    limit = 1000
    while True:
        data = ozon_post(
            client_id,
            api_key,
            "/v2/posting/fbo/list",
            {
                "dir": "asc",
                "filter": {"since": since, "to": to},
                "limit": limit,
                "offset": offset,
                "with": {"financial_data": True, "products": True},
            },
        )
        result = data.get("result")
        postings = result.get("postings") if isinstance(result, dict) else result or []
        for p in postings:
            yield p
        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.2)


def extract(posting: Dict[str, Any]):
    out = defaultdict(lambda: [0, 0.0, 0.0])
    fin = (posting.get("financial_data") or {}).get("products") or []

    for pr in fin:
        if not is_rub_product(pr, posting):
            continue

        oid = str(pr.get("offer_id") or "").strip()
        if not oid:
            continue

        qty = int(pr.get("quantity", 0) or 0)
        out[oid][0] += qty

        paid = pr.get("customer_price") or pr.get("price") or 0
        out[oid][1] += float(paid)

        out[oid][2] += float(pr.get("payout", 0) or 0)

    return out


# ================= MAIN =================

def main():
    oz1_id = os.environ["OZON1_CLIENT_ID"]
    oz1_key = os.environ["OZON1_API_KEY"]
    oz2_id = os.environ["OZON2_CLIENT_ID"]
    oz2_key = os.environ["OZON2_API_KEY"]

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_json = os.environ["GOOGLE_CREDS_JSON"]

    today = dt.date.today()
    since90 = iso_dt(today - dt.timedelta(days=90))
    since7 = today - dt.timedelta(days=7)
    to = iso_dt(today + dt.timedelta(days=1))

    gc = gspread.authorize(
        Credentials.from_service_account_file(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    )
    ws = gc.open_by_key(sheet_id).worksheet(SHEET_NAME)

    for c, v in HEADERS.items():
        ws.update(c, [[v]])

    offer_ids = ws.col_values(4)[START_ROW - 1 :]

    Q90, C90, Q7, C7, P7 = map(defaultdict, [int, float, int, float, float])

    def collect(cid, key):
        for p in list(fetch_fbs(cid, key, since90, to)) + list(fetch_fbo(cid, key, since90, to)):
            d = get_post_date(p)
            data = extract(p)
            for oid, (q, c, pay) in data.items():
                Q90[oid] += q
                C90[oid] += c
                if d and d >= since7:
                    Q7[oid] += q
                    C7[oid] += c
                    P7[oid] += pay

    collect(oz1_id, oz1_key)
    collect(oz2_id, oz2_key)

    rows = []
    for oid in offer_ids:
        q90, q7 = Q90[oid], Q7[oid]
        avg90 = round(C90[oid] / q90, 2) if q90 else ""
        avg7 = round(C7[oid] / q7, 2) if q7 else avg90
        rows.append([q90, avg90, q7, avg7, round(P7[oid], 2)])

    ws.update(f"E{START_ROW}:I{START_ROW+len(rows)-1}", rows, value_input_option="USER_ENTERED")

    formulas = [
        [f"=IFNA(I{START_ROW+i}-G{START_ROW+i}*VLOOKUP(D{START_ROW+i};'API Ozon'!F:H;3;0);\"\")"]
        for i in range(len(rows))
    ]

    ws.update(f"J{START_ROW}:J{START_ROW+len(rows)-1}", formulas, value_input_option="USER_ENTERED")

    print("OK: headers + E–J updated")


if __name__ == "__main__":
    main()
