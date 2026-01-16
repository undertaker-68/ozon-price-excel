#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Dict, List, Any

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
    r = requests.post(
        OZON_API_BASE + path,
        headers={
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=90,
    )
    if not r.ok:
        raise Exception(f"Ozon {r.status_code}: {r.text}")
    return r.json()


# ================= OZON =================

def fetch_fbs(client_id, api_key, since, to):
    offset = 0
    limit = 1000
    while True:
        data = ozon_post(client_id, api_key, "/v3/posting/fbs/list", {
            "dir": "asc",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        })
        postings = data["result"]["postings"]
        for p in postings:
            yield p
        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.2)


def fetch_fbo(client_id, api_key, since, to):
    offset = 0
    limit = 1000
    while True:
        data = ozon_post(client_id, api_key, "/v2/posting/fbo/list", {
            "dir": "asc",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        })
        postings = data["result"]["postings"]
        for p in postings:
            yield p
        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.2)


def extract(posting, offer_set):
    out = defaultdict(lambda: [0, 0.0, 0.0])  # qty, client, payout
    fin = (posting.get("financial_data") or {}).get("products") or []
    for p in fin:
        oid = str(p.get("offer_id"))
        if oid in offer_set:
            out[oid][0] += int(p.get("quantity", 0))
            out[oid][1] += float(p.get("client_price", 0))
            out[oid][2] += float(p.get("payout", 0))
    return out


# ================= MAIN =================

def main():
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
    since7 = today - dt.timedelta(days=7)
    to = iso_dt(today + dt.timedelta(days=1))

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
    offer_ids = ws.col_values(4)[START_ROW - 1:]
    offer_set = set(filter(None, offer_ids))

    # aggregates
    Q90 = defaultdict(int)
    C90 = defaultdict(float)
    Q7 = defaultdict(int)
    C7 = defaultdict(float)
    P7 = defaultdict(float)

    def collect(client_id, api_key):
        for p in list(fetch_fbs(client_id, api_key, since90, to)) + \
                 list(fetch_fbo(client_id, api_key, since90, to)):
            date = dt.datetime.fromisoformat(
                p.get("created_at", "").replace("Z", "+00:00")
            ).date()
            data = extract(p, offer_set)
            for oid, (q, c, pay) in data.items():
                Q90[oid] += q
                C90[oid] += c
                if date >= today - dt.timedelta(days=7):
                    Q7[oid] += q
                    C7[oid] += c
                    P7[oid] += pay

    collect(oz1_id, oz1_key)
    collect(oz2_id, oz2_key)

    # write F–J
    rows = []
    for oid in offer_ids:
        if not oid:
            rows.append(["", "", "", "", ""])
            continue
        q90 = Q90[oid]
        q7 = Q7[oid]
        avg90 = round(C90[oid] / q90, 2) if q90 else ""
        avg = round((C7[oid] / q7 if q7 else C90[oid] / q90), 2) if (q7 or q90) else ""
        rows.append([q90, avg90, q7, avg, round(P7[oid], 2)])

    ws.update(
        range_name=f"F{START_ROW}:J{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    # write profit formula K
    k_formulas = [[
        f'=IFNA(J{START_ROW+i}-H{START_ROW+i}*VLOOKUP(D{START_ROW+i};\'API Ozon\'!F:H;3;0);"")'
    ] for i in range(len(rows))]

    ws.update(
        range_name=f"K{START_ROW}:K{START_ROW + len(rows) - 1}",
        values=k_formulas,
        value_input_option="USER_ENTERED",
    )

    print("OK: headers + F–K updated")


if __name__ == "__main__":
    main()
