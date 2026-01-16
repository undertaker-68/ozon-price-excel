#!/usr/bin/env python3
# orders/sync_orders_to_sheet.py
# Тянет заказы (FBS+FBO) из ДВУХ кабинетов Ozon, агрегирует по offer_id и записывает в лист "Заказы Ozon":
# F: qty90, G: avg_price90, H: qty7, I: avg_price7 (если 0 -> avg90), J: payout7
#
# Требуемые ENV:
#   OZON1_CLIENT_ID, OZON1_API_KEY
#   OZON2_CLIENT_ID, OZON2_API_KEY
#   GOOGLE_SHEET_ID
#   GOOGLE_CREDS_JSON   (путь к service account json)
#
# Опционально:
#   SHEET_NAME=Заказы Ozon
#   OFFER_ID_COL=4           (по умолчанию D)
#   START_ROW=2              (по умолчанию 2)
#   DAYS_90=90
#   DAYS_7=7
#   OZON_API_BASE=https://api-seller.ozon.ru
#
# Установка:
#   pip install requests gspread google-auth

import os
import time
import datetime as dt
from collections import defaultdict
from typing import Dict, Tuple, Iterable, Any, List

import requests

import gspread
from google.oauth2.service_account import Credentials


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return str(v)


def iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_any_date(s: str) -> dt.date:
    # "2025-01-16T10:20:30Z" / "+00:00" / with milliseconds
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()


def ozon_post(base: str, client_id: str, api_key: str, path: str, payload: dict) -> dict:
    url = base.rstrip("/") + path
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()


def fetch_fbs_postings(base: str, client_id: str, api_key: str, since: str, to: str) -> Iterable[dict]:
    """
    /v3/posting/fbs/list (offset/limit) + with.financial_data
    """
    offset = 0
    limit = 1000
    while True:
        payload = {
            "dir": "ASC",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        }
        data = ozon_post(base, client_id, api_key, "/v3/posting/fbs/list", payload)
        res = (data.get("result") or {})
        postings = res.get("postings") or []
        total = res.get("total")

        for p in postings:
            yield p

        if not postings:
            break

        if total is None:
            if len(postings) < limit:
                break
            offset += limit
        else:
            offset += limit
            if offset >= int(total):
                break

        time.sleep(0.15)


def fetch_fbo_postings(base: str, client_id: str, api_key: str, since: str, to: str) -> Iterable[dict]:
    """
    Обычно встречается /v2/posting/fbo/list. Если у тебя другой путь — поменяй здесь.
    """
    offset = 0
    limit = 1000
    path = "/v2/posting/fbo/list"

    while True:
        payload = {
            "dir": "ASC",
            "filter": {"since": since, "to": to},
            "limit": limit,
            "offset": offset,
            "with": {"financial_data": True, "products": True},
        }

        data = ozon_post(base, client_id, api_key, path, payload)

        # Варианты ответа:
        # 1) {"result":{"postings":[...], "total": N}}
        # 2) {"result":[...], "total": N}
        postings = []
        total = None
        if isinstance(data.get("result"), dict):
            postings = data["result"].get("postings") or []
            total = data["result"].get("total")
        elif isinstance(data.get("result"), list):
            postings = data["result"]
            total = data.get("total")
        else:
            # fallback
            postings = (data.get("result") or {}).get("postings") or []
            total = (data.get("result") or {}).get("total")

        for p in postings:
            yield p

        if not postings:
            break

        if total is None:
            if len(postings) < limit:
                break
            offset += limit
        else:
            offset += limit
            if offset >= int(total):
                break

        time.sleep(0.15)


def get_posting_date(p: dict) -> dt.date:
    for k in ("created_at", "in_process_at", "shipment_date", "shipped_at"):
        v = p.get(k)
        if v:
            return parse_any_date(v)
    return dt.date.today()


def aggregate_posting_fast(
    posting: dict,
    offer_set: set[str],
) -> Tuple[Dict[str, int], Dict[str, float], Dict[str, float]]:
    """
    Возвращает по ОДНОМУ posting:
      qty_by_offer, client_total_by_offer, payout_total_by_offer

    Берём из financial_data.products если есть, иначе fallback из products (price*qty, payout=0).
    Важно: тут мы НЕ делаем O(n_offers) — только по товарам в posting.
    """
    qty: Dict[str, int] = defaultdict(int)
    client_total: Dict[str, float] = defaultdict(float)
    payout_total: Dict[str, float] = defaultdict(float)

    products = posting.get("products") or []
    fin_products = ((posting.get("financial_data") or {}).get("products")) or []

    # 1) qty из products (почти всегда есть)
    for pr in products:
        oid = str(pr.get("offer_id", "")).strip()
        if not oid or oid not in offer_set:
            continue
        q = int(pr.get("quantity", 0) or 0)
        if q:
            qty[oid] += q

    # 2) деньги из financial_data (предпочтительно)
    if fin_products:
        for fp in fin_products:
            oid = str(fp.get("offer_id", "")).strip()
            if not oid or oid not in offer_set:
                continue

            # если qty не нашли по products — попробуем взять отсюда
            if qty.get(oid, 0) == 0:
                q = int(fp.get("quantity", 0) or 0)
                if q:
                    qty[oid] += q

            cp = fp.get("client_price")
            po = fp.get("payout")

            # Обычно эти поля уже "за позицию" (итог по этому товару в posting).
            # Если вдруг у тебя окажется "за штуку" — будет видно по проверке на одном заказе.
            if cp is not None:
                try:
                    client_total[oid] += float(cp)
                except Exception:
                    pass
            if po is not None:
                try:
                    payout_total[oid] += float(po)
                except Exception:
                    pass

        return qty, client_total, payout_total

    # 3) fallback: если financial_data нет — берём price*qty как client_total
    # payout тут неизвестен -> 0
    for pr in products:
        oid = str(pr.get("offer_id", "")).strip()
        if not oid or oid not in offer_set:
            continue
        q = int(pr.get("quantity", 0) or 0)
        price = pr.get("price")
        if price is None:
            continue
        try:
            client_total[oid] += float(price) * q
        except Exception:
            continue

    return qty, client_total, payout_total


def collect_for_account(
    base: str,
    client_id: str,
    api_key: str,
    offer_set: set[str],
    since90: dt.date,
    since7: dt.date,
    to_date: dt.date,
) -> Tuple[Dict[str, int], Dict[str, float], Dict[str, int], Dict[str, float], Dict[str, float]]:
    """
    Возвращает агрегаты за 90 и 7 дней:
      qty90, client90, qty7, client7, payout7
    """
    qty90: Dict[str, int] = defaultdict(int)
    client90: Dict[str, float] = defaultdict(float)

    qty7: Dict[str, int] = defaultdict(int)
    client7: Dict[str, float] = defaultdict(float)
    payout7: Dict[str, float] = defaultdict(float)

    since90_s = iso_date(since90)
    to_s = iso_date(to_date)

    def consume(p: dict):
        d = get_posting_date(p)
        q_map, c_map, p_map = aggregate_posting_fast(p, offer_set)

        for oid, q in q_map.items():
            if q:
                qty90[oid] += q
        for oid, c in c_map.items():
            if c:
                client90[oid] += c

        if d >= since7:
            for oid, q in q_map.items():
                if q:
                    qty7[oid] += q
            for oid, c in c_map.items():
                if c:
                    client7[oid] += c
            for oid, po in p_map.items():
                if po:
                    payout7[oid] += po

    # FBS
    for p in fetch_fbs_postings(base, client_id, api_key, since90_s, to_s):
        consume(p)

    # FBO
    for p in fetch_fbo_postings(base, client_id, api_key, since90_s, to_s):
        consume(p)

    return qty90, client90, qty7, client7, payout7


def gspread_client(creds_path: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def read_offer_ids(ws: gspread.Worksheet, offer_col: int, start_row: int) -> List[str]:
    col_vals = ws.col_values(offer_col)
    offer_ids: List[str] = []
    for v in col_vals[start_row - 1:]:
        vv = str(v).strip()
        offer_ids.append(vv)  # сохраняем позиционно (пустые тоже), чтобы совпало по строкам
    return offer_ids


def write_range(ws: gspread.Worksheet, start_row: int, start_col: int, values: List[List[Any]]):
    if not values:
        return
    end_row = start_row + len(values) - 1
    end_col = start_col + len(values[0]) - 1
    a1_start = gspread.utils.rowcol_to_a1(start_row, start_col)
    a1_end = gspread.utils.rowcol_to_a1(end_row, end_col)
    rng = f"{a1_start}:{a1_end}"
    ws.update(rng, values, value_input_option="USER_ENTERED")


def main():
    base = os.environ.get("OZON_API_BASE", "https://api-seller.ozon.ru")

    oz1_client = _env("OZON1_CLIENT_ID")
    oz1_key = _env("OZON1_API_KEY")
    oz2_client = _env("OZON2_CLIENT_ID")
    oz2_key = _env("OZON2_API_KEY")

    sheet_id = _env("GOOGLE_SHEET_ID")
    creds_json = _env("GOOGLE_CREDS_JSON")

    sheet_name = os.environ.get("SHEET_NAME", "Заказы Ozon")
    offer_col = int(os.environ.get("OFFER_ID_COL", "4"))  # D
    start_row = int(os.environ.get("START_ROW", "2"))

    days_90 = int(os.environ.get("DAYS_90", "90"))
    days_7 = int(os.environ.get("DAYS_7", "7"))

    gc = gspread_client(creds_json)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)

    offer_ids_positional = read_offer_ids(ws, offer_col=offer_col, start_row=start_row)

    # offer_set для агрегаций (только непустые)
    offer_set = {oid for oid in offer_ids_positional if oid}
    if not offer_set:
        print("No offer_id found in column D (or specified OFFER_ID_COL). Nothing to do.")
        return

    today = dt.date.today()
    since90 = today - dt.timedelta(days=days_90)
    since7 = today - dt.timedelta(days=days_7)
    to_date = today + dt.timedelta(days=1)  # включаем "сегодня"

    # Общие агрегаты по двум кабинетам (суммируем)
    Q90: Dict[str, int] = defaultdict(int)
    C90: Dict[str, float] = defaultdict(float)
    Q7: Dict[str, int] = defaultdict(int)
    C7: Dict[str, float] = defaultdict(float)
    P7: Dict[str, float] = defaultdict(float)

    for idx, (cid, key) in enumerate([(oz1_client, oz1_key), (oz2_client, oz2_key)], start=1):
        print(f"[Cab{idx}] Fetching postings...")
        q90, c90, q7, c7, p7 = collect_for_account(
            base=base,
            client_id=cid,
            api_key=key,
            offer_set=offer_set,
            since90=since90,
            since7=since7,
            to_date=to_date,
        )
        for k, v in q90.items():
            Q90[k] += v
        for k, v in c90.items():
            C90[k] += v
        for k, v in q7.items():
            Q7[k] += v
        for k, v in c7.items():
            C7[k] += v
        for k, v in p7.items():
            P7[k] += v

    # Формируем выход F:J в порядке строк листа (позиционно)
    out: List[List[Any]] = []
    for oid in offer_ids_positional:
        if not oid:
            out.append(["", "", "", "", ""])
            continue

        q90 = int(Q90.get(oid, 0))
        avg90 = (C90.get(oid, 0.0) / q90) if q90 else 0.0

        q7 = int(Q7.get(oid, 0))
        avg7 = (C7.get(oid, 0.0) / q7) if q7 else 0.0

        avg_i = avg7 if q7 else (avg90 if q90 else 0.0)
        payout7 = float(P7.get(oid, 0.0))

        # F qty90
        # G avg90
        # H qty7
        # I avg7_or_90
        # J payout7
        out.append([
            q90,
            round(avg90, 2) if q90 else "",
            q7,
            round(avg_i, 2) if (q7 or q90) else "",
            round(payout7, 2) if payout7 else 0,
        ])

    # Пишем в F2:J
    start_col_f = 6  # F
    write_range(ws, start_row=start_row, start_col=start_col_f, values=out)
    print(f"Done. Updated {sheet_name}!F{start_row}:J{start_row + len(out) - 1}")


if __name__ == "__main__":
    main()
