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
    # колонка E удалена -> блок показателей начинается с E
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
    # created_at у Ozon не всегда есть/полезный -> берём первый доступный
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
    # помогает если где-то "00512", а где-то "512"
    s = (oid or "").strip()
    if s.isdigit():
        try:
            return str(int(s))
        except Exception:
            return s
    return s


def is_rub(code: Any) -> bool:
    # Правило A: если валюты нет -> считаем RUB
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


# ================= EXTRACT =================

def extract(posting: Dict[str, Any]) -> Dict[str, Any]:
    """
    Возвращает {offer_id: (qty, paid_total, payout_total)} для одного posting.

    paid_total = "Оплачено покупателем" (по приоритету):
      1) financial_data.products[].customer_price
      2) posting.products[].price
      3) financial_data.products[].price

    Важно: offer_id/qty обычно лежат в posting["products"], а в financial_data часто только product_id.
    Поэтому матчим по sku/product_id.
    """
    out = defaultdict(lambda: [0, 0.0, 0.0])

    prod_rows = posting.get("products") or []
    fin_rows = (posting.get("financial_data") or {}).get("products") or []

    # index financial by product_id (он соответствует sku в posting.products)
    fin_by_pid: Dict[int, Dict[str, Any]] = {}
    for fr in fin_rows:
        pid = to_int(fr.get("product_id"))
        if pid:
            fin_by_pid[pid] = fr

    for prod in prod_rows:
        oid = str(prod.get("offer_id") or "").strip()
        if not oid:
            continue

        sku = to_int(prod.get("sku"))
        fin = fin_by_pid.get(sku, {}) if sku else {}

        # валюта: из prod или fin; правило A (нет -> RUB)
        cur = prod.get("currency_code")
        if cur is None or str(cur).strip() == "":
            cur = fin.get("currency_code")
        if not is_rub(cur):
            continue

        qty = to_int(prod.get("quantity"))
        if qty <= 0:
            # иногда qty только в фин.данных
            qty = to_int(fin.get("quantity"))
        if qty <= 0:
            qty = 0

        # "Оплачено покупателем"
        paid = fin.get("customer_price")
        if paid is None or str(paid).strip() == "":
            paid = prod.get("price")
        if paid is None or str(paid).strip() == "":
            paid = fin.get("price")
        paid_f = to_float(paid)

        payout_f = to_float(fin.get("payout"))

        # сохраняем под оригинальным и нормализованным ключом, чтобы бить "00512"/"512"
        for key in {oid, norm_offer_id(oid)}:
            if not key:
                continue
            out[key][0] += qty
            out[key][1] += paid_f
            out[key][2] += payout_f

    return out


def get_metric(d: Dict[str, Any], oid: str, default: Any = 0) -> Any:
    s = (oid or "").strip()
    if s in d:
        return d[s]
    ns = norm_offer_id(s)
    if ns in d:
        return d[ns]
    return default


# ================= MAIN =================

def main() -> None:
    oz1_id = os.environ["OZON1_CLIENT_ID"]
    oz1_key = os.environ["OZON1_API_KEY"]
    oz2_id = os.environ["OZON2_CLIENT_ID"]
    oz2_key = os.environ["OZON2_API_KEY"]

    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_json = os.environ["GOOGLE_CREDS_JSON"]

    today = dt.date.today()
    since90 = iso_dt(today - dt.timedelta(days=90))
    to = iso_dt(today + dt.timedelta(days=1))
    since7_date = today - dt.timedelta(days=7)

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

    # offer_id column D
    offer_ids = ws.col_values(4)[START_ROW - 1 :]

    Q90 = defaultdict(int)
    C90 = defaultdict(float)
    Q7 = defaultdict(int)
    C7 = defaultdict(float)
    P7 = defaultdict(float)

    def collect_account(client_id: str, api_key: str) -> None:
        for p in fetch_fbs(client_id, api_key, since90, to):
            post_date = get_post_date(p)
            data = extract(p)
            for oid, (q, paid, pay) in data.items():
                Q90[oid] += q
                C90[oid] += paid
                if post_date and post_date >= since7_date:
                    Q7[oid] += q
                    C7[oid] += paid
                    P7[oid] += pay

        for p in fetch_fbo(client_id, api_key, since90, to):
            post_date = get_post_date(p)
            data = extract(p)
            for oid, (q, paid, pay) in data.items():
                Q90[oid] += q
                C90[oid] += paid
                if post_date and post_date >= since7_date:
                    Q7[oid] += q
                    C7[oid] += paid
                    P7[oid] += pay

    collect_account(oz1_id, oz1_key)
    collect_account(oz2_id, oz2_key)

    # write E–I
    rows = []
    for oid in offer_ids:
        oid = (oid or "").strip()
        if not oid:
            rows.append(["", "", "", "", ""])
            continue

        q90 = get_metric(Q90, oid, 0)
        q7 = get_metric(Q7, oid, 0)

        c90 = get_metric(C90, oid, 0.0)
        c7 = get_metric(C7, oid, 0.0)
        p7 = get_metric(P7, oid, 0.0)

        avg90 = round(c90 / q90, 2) if q90 else ""
        if q7:
            avg7 = round(c7 / q7, 2)
        elif q90:
            avg7 = round(c90 / q90, 2)
        else:
            avg7 = ""

        rows.append([q90, avg90, q7, avg7, round(p7, 2)])

    ws.update(
        range_name=f"E{START_ROW}:I{START_ROW + len(rows) - 1}",
        values=rows,
        value_input_option="USER_ENTERED",
    )

    # profit formula in J
    formulas = [
        [f'=IFNA(I{START_ROW+i}-G{START_ROW+i}*VLOOKUP(D{START_ROW+i};\'API Ozon\'!F:H;3;0);"")']
        for i in range(len(rows))
    ]
    ws.update(
        range_name=f"J{START_ROW}:J{START_ROW + len(rows) - 1}",
        values=formulas,
        value_input_option="USER_ENTERED",
    )

    print("OK: headers + E–J updated")


if __name__ == "__main__":
    main()
