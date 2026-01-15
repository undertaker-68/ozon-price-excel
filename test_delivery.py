import json
from pathlib import Path
from playwright.sync_api import sync_playwright

PROJECT = Path(__file__).resolve().parent
COOKIES_TXT = PROJECT / "cookies.txt"

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

def deep_find_first(obj, want_keys):
    """Возвращает первый dict, который содержит хотя бы один ключ из want_keys."""
    if isinstance(obj, dict):
        if any(k in obj for k in want_keys):
            return obj
        for v in obj.values():
            r = deep_find_first(v, want_keys)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = deep_find_first(it, want_keys)
            if r is not None:
                return r
    return None

def main():
    if not COOKIES_TXT.exists():
        raise SystemExit(f"Нет cookies файла: {COOKIES_TXT}")

    cookies = parse_netscape_cookies(
        COOKIES_TXT.read_text(encoding="utf-8", errors="ignore")
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

        # ВАЖНО: сначала зайдём на seller.ozon.ru, чтобы контекст подхватил куки корректно
        page = context.new_page()
        page.goto("https://seller.ozon.ru/", wait_until="domcontentloaded")

        # Теперь делаем прямой запрос в этом же браузерном контексте
        resp = context.request.get(API_URL)
        status = resp.status
        text = resp.text()

        browser.close()

    # Сохраним ответ для диагностики
    out = PROJECT / "last_delivery.json"
    out.write_text(text, encoding="utf-8", errors="ignore")
    print("Saved response to:", out)
    print("HTTP:", status)

    # Попробуем распарсить JSON
    j = json.loads(text)

    # Найдём ВСЕ точки, где есть date + tariff/averageDeliveryTime
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
        raise SystemExit("JSON получен, но не нашёл точки с date+tariff/averageDeliveryTime. См. last_delivery.json")

    # Берём самую свежую по строке даты YYYY-MM-DD
    points.sort(key=lambda x: x.get("date", ""))
    last = points[-1]

    tariff = last.get("tariff") or {}
    tariff_value = tariff.get("tariffValue")
    fee = tariff.get("fee")
    avg = last.get("averageDeliveryTime")

    print("OK")
    print("date:", last.get("date"))
    print("averageDeliveryTime:", avg)
    print("tariffValue:", tariff_value)
    print("fee:", fee)

if __name__ == "__main__":
    main()
