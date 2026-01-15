#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

from ozon_delivery import get_latest_average_delivery_metrics


# ───────────────────────────────────────────────────────────
# Константы
# ───────────────────────────────────────────────────────────

PROJECT_DIR = Path(__file__).resolve().parent
COOKIES_FILE = PROJECT_DIR / "cookies.txt"

LOCK_FILE = "/var/lib/ozon/avg_delivery.lock"


# ───────────────────────────────────────────────────────────
# Lock: 1 раз в сутки
# ───────────────────────────────────────────────────────────

def daily_lock(lock_path: str) -> bool:
    """
    True  — можно выполнять сегодня (и ставим метку)
    False — уже выполняли сегодня
    """
    p = Path(lock_path)
    today = date.today().isoformat()

    if p.exists():
        try:
            if p.read_text(encoding="utf-8").strip() == today:
                return False
        except Exception:
            pass

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(today, encoding="utf-8")
    return True


# ───────────────────────────────────────────────────────────
# Google Sheets
# ───────────────────────────────────────────────────────────

def connect_sheet(service_account_json: str, spreadsheet_id: str, worksheet_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    return ws


# ───────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────

def main():
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("WORKSHEET_NAME", "").strip() or "API Ozon"
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if not spreadsheet_id:
        raise SystemExit("SPREADSHEET_ID is required")

    if not service_account_json or not os.path.exists(service_account_json):
        raise SystemExit(f"GOOGLE_SERVICE_ACCOUNT_JSON not found: {service_account_json}")

    if not COOKIES_FILE.exists():
        raise SystemExit(f"cookies.txt not found: {COOKIES_FILE}")

    # ─── lock ───────────────────────────────────────────────
    if not daily_lock(LOCK_FILE):
        print("avg-delivery: already fetched today, skip")
        return

    # ─── Получаем проценты с Ozon (1 запрос) ────────────────
    m = get_latest_average_delivery_metrics(COOKIES_FILE)

    val_r = (m.get("tariffValue") or 0) / 100.0
    val_s = (m.get("fee") or 0) / 100.0

    print("avg-delivery metrics:", m)

    # ─── Подключаемся к таблице ─────────────────────────────
    ws = connect_sheet(service_account_json, spreadsheet_id, worksheet_name)

    # Определяем последнюю строку по колонке A
    colA = ws.col_values(1)
    last_row = len(colA)

    if last_row < 3:
        print("No data rows (need at least row 3).")
        return

    # ─── Заголовки ─────────────────────────────────────────
    HEADER_ROW = 2
    DATA_START_ROW = 3

    ws.update(
        range_name=f"R{HEADER_ROW}:S{HEADER_ROW}",
        values=[["% к лог", "% от цены"]],
        value_input_option="USER_ENTERED",
    )

    # ─── Данные ────────────────────────────────────────────
    nrows = last_row - (DATA_START_ROW - 1)
    values = [[val_r, val_s] for _ in range(nrows)]

    rng = f"R{DATA_START_ROW}:S{last_row}"
    ws.update(
        range_name=rng,
        values=values,
        value_input_option="USER_ENTERED",
    )

    print(f"Wrote {nrows} rows to {rng}: R={val_r} S={val_s}")


# ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
