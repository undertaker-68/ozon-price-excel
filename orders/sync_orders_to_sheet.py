import os
import time
import math
import datetime as dt
from collections import defaultdict

import requests

# --- Google Sheets (через gspread) ---
import gspread
from google.oauth2.service_account import Credentials


OZON_API_BASE = "https://api-seller.ozon.ru"


def iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def ozon_post(client_id: str, api_key: str, path: str, payload: dict) -> dict:
    url = OZON_API_BASE + path
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_fbs_postings(client_id: str, api_key: str, since: str, to: str):
    """
    /v3/posting/fbs/list supports offset/limit and with.financial_data
    """
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "ASC",
            "filter": {
                "since": since,
                "to": to,
            },
            "limit": limit,
            "offset": offset,
            "with": {
                "financial_data": True,
                "products": True,
            },
        }
        data = ozon_post(client_id, api_key, "/v3/posting/fbs/list", payload)
        postings = data.get("result", {}).get("postings", []) or []
        for p in postings:
            yield p

        total = data.get("result", {}).get("total", None)
        if total is None:
            # если total нет — ориентируемся на размер страницы
            if len(postings) < limit:
                break
        else:
            offset += limit
            if offset >= total:
                break

        if not postings:
            break
        time.sleep(0.2)


def fetch_fbo_postings(client_id: str, api_key: str, since: str, to: str):
    """
    В разных аккаунтах бывает /v2/posting/fbo/list или /v2/posting/fbo/list (актуальный у Ozon).
    Попробуем сначала v2, если 404 — можно быстро поменять путь.
    """
    offset = 0
    limit = 1000
    path = "/v2/posting/fbo/list"

    while True:
        payload = {
            "dir": "ASC",
            "filter": {
                "since": since,
                "to": to,
            },
            "limit": limit,
            "offset": offset,
            "with": {
                "financial_data": True,
                "products": True,
            },
        }

        try:
            data = ozon_post(client_id, api_key, path, payload)
        except requests.HTTPError as e:
            # если вдруг эндпоинт другой — покажем понятную ошибку
            raise

        postings = data.get("result", []) or data.get("result", {}).get("postings", []) or []
        # В FBO форматы иногда отличаются: бывает result = {"postings":[...], "total":...}
        if isinstance(data.get("result"), dict):
            postings = data["result"].get("postings", []) or []
            total = data["result"].get("total", None)
        else:
            total = data.get("total", None)

        for p in postings:
            yield p

        if total is None:
            if len(postings) < limit:
                break
        else:
            offset += limit
            if offset >= total:
                break

        if not postings:
            break
        time.sleep(0.2)


def posting_date(p: dict) -> dt.date:
    # Берём created_at если есть, иначе in_process_at / shipped_at
    for k in ("created_at", "in_process_at", "shipment_date", "shipped_at"):
        v = p.get(k)
        if v:
            # "2025-01-16T10:20:30Z" или с миллисекундами
            return dt.datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    # fallback: сегодня
    return dt.date.today()


def sum_financial_for_offer(posting: dict, offer_id: str):
    """
    Пытаемся вытащить:
    - qty
    - client_price_total (сумма, которую заплатил покупатель)
    - payout_total (что выплатит Ozon)
    По возможности из financial_data.products, иначе из products.
    """
    qty = 0
    client_total = 0.0
    payout_total = 0.0

    products = posting.get("products") or []
    # qty из products
    for pr in products:
        if str(pr.get("offer_id", "")).strip() == offer_id:
            q = pr.get("quantity", 0) or 0
            qty += int(q)

    fin = (posting.get("financial_data") or {}).get("products") or []
    # В fin обычно есть offer_id, quantity, client_price, payout
    fin_found = False
    for fp in fin:
        if str(fp.get("offer_id", "")).strip() == offer_id:
            fin_found = True
            q = fp.get("quantity", 0) or 0
            # если qty не нашли выше — берём отсюда
            if qty == 0:
                qty += int(q)

            client_price = fp.get("client_price")
            payout = fp.get("payout")

            # Иногда payout/client_price могут быть за единицу или за позицию — чаще за позицию.
            # Здесь считаем "как есть" (за позицию), но если q>1 и значения "за единицу", легко поправить.
            if client_price is not None:
                client_total += float(client_price)
            if payout is not None:
                payout_total += float(payout)

    # Если financial_data нет — fallback: можно взять price из products (но это хуже)
    if not fin_found:
        # попробуем pr["price"] как цену за штуку
        for pr in products:
            if str(pr.get("offer_id", "")).strip() == offer_id:
                q = int(pr.get("quantity", 0) or 0)
                price = pr.get("price")
                if price is not None:
                    client_total += float(price) * q

    return qty, client_total, payout_total


def read_offer_ids(gc: gspread.Client, sheet_id: str, sheet_name: str = "Заказы Ozon"):
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)
    # D2:D — offer_id
    col = ws.col_values(4)  # D
    offer_ids = []
    for v in col[1:]:  # skip header
        v = str(v).strip()
        if v:
            offer_ids.append(v)
    return ws, offer_ids


def write_metrics(ws: gspread.Worksheet, values_f_to_j):
    """
    values_f_to_j: list of rows, each row = [F, G, H, I, J]
    """
    # Пишем начиная с F2
    start_row = 2
    start_col = 6  # F
    end_row = start_row + len(values_f_to_j) - 1
    end_col = 10  # J
    rng = gspread.utils.rowcol_to_a1(start_row, start_col) + ":" + gspread.utils.rowcol_to_a1(end_row, end_col)
    ws.update(rng, values_f_to_j, value_input_option="USER_ENTERED")


def main():
    client_id = os.environ["OZON_CLIENT_ID"]
    api_key = os.environ["OZON_API_KEY"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_json = os.environ["GOOGLE_CREDS_JSON"]  # путь к service account json

    creds = Credentials.from_service_account_file(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)

    ws, offer_ids = read_offer_ids(gc, sheet_id, "Заказы Ozon")
    if not offer_ids:
        print("No offer_id found in column D")
        return

    today = dt.date.today()
    since90 = iso_date(today - dt.timedelta(days=90))
    since7 = today - dt.timedelta(days=7)
    to = iso_date(today + dt.timedelta(days=1))  # до завтра, чтобы включить сегодня

    # агрегаторы: offer_id -> sums
    qty90 = defaultdict(int)
    client90 = defaultdict(float)

    qty7 = defaultdict(int)
    client7 = defaultdict(float)
    payout7 = defaultdict(float)

    offer_set = set(offer_ids)

    def consume_posting(p):
        d = posting_date(p)
        for oid in offer_set:
            q, client_total, payout_total = sum_financial_for_offer(p, oid)
            if q <= 0 and client_total == 0 and payout_total == 0:
                continue

            qty90[oid] += q
            client90[oid] += client_total

            if d >= since7:
                qty7[oid] += q
                client7[oid] += client_total
                payout7[oid] += payout_total

    print("Fetching FBS postings...")
    for p in fetch_fbs_postings(client_id, api_key, since90, to):
        consume_posting(p)

    print("Fetching FBO postings...")
    for p in fetch_fbo_postings(client_id, api_key, since90, to):
        consume_posting(p)

    # готовим строки F-J в порядке offer_ids на листе
    out = []
    for oid in offer_ids:
        q90 = qty90.get(oid, 0)
        avg90 = (client90.get(oid, 0.0) / q90) if q90 else 0

        q7 = qty7.get(oid, 0)
        avg7 = (client7.get(oid, 0.0) / q7) if q7 else 0

        avg_for_i = avg7 if q7 else (avg90 if q90 else 0)
        payout = payout7.get(oid, 0.0)

        # F qty90, G avg90, H qty7, I avg7_or_90, J payout7
        out.append([q90, round(avg90, 2) if q90 else "", q7, round(avg_for_i, 2) if (q7 or q90) else "", round(payout, 2) if payout else 0])

    write_metrics(ws, out)
    print("Done. Updated F2:J.")


if __name__ == "__main__":
    main()
