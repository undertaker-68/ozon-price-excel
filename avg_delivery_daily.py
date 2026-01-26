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
CACHE_FILE = "/var/lib/ozon/avg_delivery_cache.json"


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


def load_cached_metrics(cache_path: str) -> dict:
    """Возвращает кеш метрик за сегодня, если он существует."""
    p = Path(cache_path)
    if not p.exists():
        return {}
    try:
        import json

        data = json.loads(p.read_text(encoding="utf-8") or "{}")
        if data.get("date") == date.today().isoformat() and isinstance(data.get("metrics"), dict):
            return data["metrics"]
    except Exception:
        return {}
    return {}


def save_cached_metrics(cache_path: str, metrics: dict) -> None:
    """Сохраняем метрики на сегодня, чтобы можно было перезаписать ячейки повторным запуском."""
    try:
        import json

        p = Path(cache_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {"date": date.today().isoformat(), "metrics": metrics}
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        # кеш — не критично
        pass


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
    # 1 раз в сутки НЕ делаем сетевой запрос, но при повторном запуске
    # всё равно заполняем лист из кеша (иначе после очистки ячеек они останутся пустыми).
    if daily_lock(LOCK_FILE):
        m = get_latest_average_delivery_metrics(COOKIES_FILE)
        save_cached_metrics(CACHE_FILE, m)
    else:
        m = load_cached_metrics(CACHE_FILE)
        if not m:
            print("avg-delivery: already fetched today, and no cache found — skip")
            return

    # ВАЖНО:
    # В листе API Ozon колонка R занята под "Баз лог" (см. sync.py).
    # Поэтому метрики с seller.ozon.ru (tariffValue/fee) пишем в S и T.
    val_s = (m.get("tariffValue") or 0) / 100.0   # % к логистике
    val_t = (m.get("fee") or 0) / 100.0           # % от цены

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
        range_name=f"S{HEADER_ROW}:T{HEADER_ROW}",
        values=[["% к лог", "% от цены"]],
        value_input_option="USER_ENTERED",
    )

    # ─── Данные ────────────────────────────────────────────
    nrows = last_row - (DATA_START_ROW - 1)
    values = [[val_s, val_t] for _ in range(nrows)]

    rng = f"S{DATA_START_ROW}:T{last_row}"
    ws.update(
        range_name=rng,
        values=values,
        value_input_option="USER_ENTERED",
    )

    print(f"Wrote {nrows} rows to {rng}: S={val_s} T={val_t}")


# ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
