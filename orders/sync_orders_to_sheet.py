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
    "F1": "Заказы 90 дней, шт",
    "G1": "Средняя цена 90 дней",
    "H1": "Заказы 7 дней, шт",
    "I1": "Средняя цена для покупателя",
    "J1": "Итого получено от Ozon (7 дней)",
    "K1": "Чистая прибыль (7 дней)",
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
    """Берём первую доступную дату из набора полей, т.к. created_at часто пустой/неподходящий."""
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
    """Правило A: если кода валюты нет — считаем RUB. Если есть и != RUB — пропускаем."""
    code = (
        product_row.get("currency_code")
        or product_row.get("currency")
        or product_row.get("curr_code")
        or (posting.get("financial_data") or {}).get("currency_code")
        or posting.get("currency_code")
    )
    if code is None or str(code).strip() == "":
        return True
    return str(code).upper() == "RUB"


# ================= OZON =================

def fetch_fbs(client_id: str, api_key: str, since: str, to: str) -> Iterable[Dict[str, Any]]:
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


def fetch_fbo(client_id: str, api_key: str, since: str, to: str) -> Iterable[Dict[str, Any]]:
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
        if isinstance(result, dict):
            postings = result.get("postings") or []
        else:
            postings = result or []

        for p in postings:
            yield p

        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.2)


def extract(posting: Dict[str, Any], offer_set: set) -> Dict[str, Any]:
    """Возвращает {offer_id: (qty, client_paid_total, payout_total)} для одного posting."""
    out = defaultdict(lambda: [0, 0.0, 0.0])
    fin_products = (posting.get("financial_data") or {}).get("products") or []

    for pr in fin_products:
        if not is_rub_product(pr, posting):
            continue

        oid = str(pr.get("offer_id") or "").strip()
        if not oid or oid not in offer_set:
            continue

        q = int(pr.get("quantity", 0) or 0)
        out[oid][0] += q
        out[oid][1] += float(pr.get("client_price", 0) or 0)  # "Оплачено покупателем"
        out[oid][2] += float(pr.get("payout", 0) or 0)

    return out


# ================= MAIN =================

def main() -> None:
    # env
    oz1_id = os.environ["OZON1_CLIENT_ID"]
    oz1_key = os.environ["OZON1_API_KEY"]
    oz2_id = os.environ["OZON2_CLIENT_ID"]
    oz2_key = os.environ["OZON2_API_KEY"]

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_json = os.environ["GOOGLE_CREDS_JSON"]

    # dates
    today = dt.date.today()
    since90 = iso_dt(today - dt.timedelta(days=90))
    to = iso_dt(today + dt.timedelta(days=1))
    since7_date = today - dt.timedelta(days=7)

    # sheets
    gc = gspread.authorize(
        Credentials.from_service_account_file(
            creds_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    )
    ws = gc.open_by_key(sheet_id).worksheet(SHEET_NAME)

    # write headers
    for cell, val in HEADERS.items():
        ws.update(range_name=cell, values=[[val]])

    # read offer_id
    offer_ids = ws.col_values(4)[START_ROW - 1 :]
    offer_set = set(filter(None, offer_ids))

    # aggregates
    Q90 = defaultdict(int)
    C90 = defaultdict(float)
    Q7 = defaultdict(int)
    C7 = defaultdict(float)
    P7 = defaultdict(float)

    def collect_account(client_id: str, api_key: str) -> None:
        for p in fetch_fbs(client_id, api_key, since90, to):
            post_date = get_post_date(p)
            data = extract(p, offer_set)
            for oid, (q, c, pay) in data.items():
                Q90[oid] += q
                C90[oid] += c
                if post_date and post_date >= since7_date:
                    Q7[oid] += q
                    C7[oid] += c
                    P7[oid] += pay

        for p in fetch_fbo(client_id, api_key, since90, to):
            post_date = get_post_date(p)
            data = extract(p, offer_set)
            for oid, (q, c, pay) in data.items():
                Q90[oid] += q
                C90[oid] += c
                if post_date and post_date >= since7_date:
                    Q7[oid] += q
                    C7[oid] += c
                    P7[oid] += pay

    collect_account(oz1_id, oz1_key)
    collect_account(oz2_id, oz2_key)

    # write F–J
    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", "", ""])
            continue

        q90 = Q90[oid]
        q7 = Q7[oid]

        avg90 = round(C90[oid] / q90, 2) if q90 else ""
        if q7:
            avg7 = round(C7[oid] / q7, 2)
        elif q90:
            avg7 = round(C90[oid] / q90, 2)
        else:
            avg7 = ""

        rows.append([q90, avg90, q7, avg7, round(P7[oid], 2)])

    ws.update(
        range_name=f"F{START_ROW}:J{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    # write profit formula K
    k_formulas = [
        [
            f"=IFNA(J{START_ROW+i}-H{START_ROW+i}*VLOOKUP(D{START_ROW+i};'API Ozon'!F:H;3;0);\"\")"
        ]
        for i in range(len(rows))
    ]

    ws.update(
        range_name=f"K{START_ROW}:K{START_ROW + len(rows) - 1}",
        values=k_formulas,
        value_input_option="USER_ENTERED",
    )

    print("OK: headers + F–K updated")


if __name__ == "__main__":
    main()
