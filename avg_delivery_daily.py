#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

from ozon_delivery import get_latest_average_delivery_metrics  # твой рабочий модуль :contentReference[oaicite:2]{index=2}

PROJECT_DIR = Path(__file__).resolve().parent
COOKIES_FILE = PROJECT_DIR / "cookies.txt"

LOCK_FILE = "/var/lib/ozon/avg_delivery.lock"

def daily_lock(lock_path: str) -> bool:
    """True = можно выполнять сегодня (и мы ставим метку). False = уже выполняли сегодня."""
    p = Path(lock_path)
    today = date.today().isoformat()
    if p.exists() and p.read_text(encoding="utf-8", errors="ignore").strip() == today:
        return False
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(today, encoding="utf-8")
    return True

def connect_sheet(service_account_json: str, spreadsheet_id: str, worksheet_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    return ws

def main():
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("WORKSHEET_NAME", "API Ozon").strip()
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if not spreadsheet_id:
        raise SystemExit("SPREADSHEET_ID is required")
    if not service_account_json or not os.path.exists(service_account_json):
        raise SystemExit(f"GOOGLE_SERVICE_ACCOUNT_JSON not found: {service_account_json}")
    if not COOKIES_FILE.exists():
        raise SystemExit(f"cookies.txt not found: {COOKIES_FILE}")

    if not daily_lock(LOCK_FILE):
        print("avg-delivery: already fetched today, skip")
        return

    # 1) Забираем проценты (seller.ozon.ru) — 1 раз/сутки
    m = get_latest_average_delivery_metrics(COOKIES_FILE)
    # Ozon даёт 40 и 2, в таблицу надо 0.40 и 0.02 (под процентный формат)
    val_r = (m["tariffValue"] or 0) / 100.0
    val_s = (m["fee"] or 0) / 100.0
    print("avg-delivery metrics:", m)

    # 2) Пишем в Google Sheet: колонка R (18), S (19)
    ws = connect_sheet(service_account_json, spreadsheet_id, worksheet_name)

    # Сколько строк с данными? (по колонке A обычно есть значения)
    colA = ws.col_values(1)  # A
    # считаем строки с 1-й: [0] — заголовок, данные обычно с 2-й
    last_row = len(colA)
    if last_row < 2:
        print("Sheet seems empty (no data rows). Write only headers row 2 is absent.")
        return

    nrows = last_row - 1  # количество строк данных (со 2-й по last_row)
    values = [[val_r, val_s] for _ in range(nrows)]

    rng = f"R2:S{last_row}"
    ws.update(rng, values, value_input_option="USER_ENTERED")

    print(f"Wrote {nrows} rows to {rng}: R={val_r} S={val_s}")

if __name__ == "__main__":
    main()
