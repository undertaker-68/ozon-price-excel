import json
from pathlib import Path
from playwright.sync_api import sync_playwright

PROJECT = Path(__file__).resolve().parent

COMPANY_ID = "151812"
API_PATH = "/api/site/seller-analytics/average-delivery-time/dynamic-chart"
API_URL = f"https://seller.ozon.ru{API_PATH}?__rr=3"

def parse_netscape_cookies(txt: str):
    cookies = []
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 7:
            continue
        domain, flag, path, secure, expiry, name, value = parts
        cookies.append({
            "name": name,
            "value": value,
            "domain": domain,
            "path": path,
            "secure": (secure.upper() == "TRUE"),
            "httpOnly": False,
        })
    return cookies

def get_latest_average_delivery_metrics(cookies_txt_path: Path) -> dict:
    """
    Возвращает самые свежие значения по максимальной date:
    {
      "date": "2026-01-15",
      "averageDeliveryTime": 37,
      "tariffValue": 40,
      "fee": 2
    }
    """
    cookies = parse_netscape_cookies(
        cookies_txt_path.read_text(encoding="utf-8", errors="ignore")
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="ru-RU",
            extra_http_headers={
                "accept": "application/json, text/plain, */*",
                "x-o3-app-name": "seller-ui",
                "x-o3-language": "ru",
                "x-o3-company-id": COMPANY_ID,
                "x-o3-page-type": "analytics_metrics",
                "referer": "https://seller.ozon.ru/app/analytics/sales-geography/local-packaging?__rr=3",
            },
        )
        context.add_cookies([c for c in cookies if "ozon.ru" in c["domain"]])

        page = context.new_page()
        page.goto("https://seller.ozon.ru/", wait_until="domcontentloaded")

        resp = context.request.get(API_URL)
        status = resp.status
        text = resp.text()
        browser.close()

    # быстрый фейл, если не 200
    if status != 200:
        raise RuntimeError(f"HTTP {status}: {text[:200]}")

    j = json.loads(text)

    # если ошибка API
    if isinstance(j, dict) and "error" in j:
        raise RuntimeError(f"API error: {j['error']}")

    # собираем все точки с date + tariff/averageDeliveryTime
    points = []

    def walk(o):
        if isinstance(o, dict):
            if "date" in o and (("tariff" in o) or ("averageDeliveryTime" in o)):
                points.append(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(j)
    if not points:
        raise RuntimeError("JSON есть, но не найден ряд с date+tariff/averageDeliveryTime")

    points.sort(key=lambda x: x.get("date", ""))
    last = points[-1]
    tariff = last.get("tariff") or {}

    return {
        "date": last.get("date"),
        "averageDeliveryTime": last.get("averageDeliveryTime"),
        "tariffValue": tariff.get("tariffValue"),
        "fee": tariff.get("fee"),
    }
