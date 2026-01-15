import json
import re
from pathlib import Path
from playwright.sync_api import sync_playwright
from datetime import datetime

PROJECT = Path(__file__).resolve().parent
COOKIES_TXT = PROJECT / "cookies.txt"

TARGET_PAGE = "https://seller.ozon.ru/app/analytics/sales-geography/local-packaging?__rr=3"
API_SUBSTR = "/api/site/seller-analytics/average-delivery-time/dynamic-chart"

def parse_netscape_cookies(txt: str):
    """
    cookies.txt (Netscape format):
    domain \t flag \t path \t secure \t expiry \t name \t value
    """
    cookies = []
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 7:
            continue
        domain, flag, path, secure, expiry, name, value = parts
        # Playwright expects no leading dot sometimes, but it's ok either way
        cookies.append({
            "name": name,
            "value": value,
            "domain": domain,
            "path": path,
            "httpOnly": False,  # Netscape doesn't store it reliably; ok for auth cookies too
            "secure": (secure.upper() == "TRUE"),
        })
    return cookies

def main():
    if not COOKIES_TXT.exists():
        raise SystemExit(f"Нет cookies файла: {COOKIES_TXT}")

    cookies_raw = COOKIES_TXT.read_text(encoding="utf-8", errors="ignore")
    cookies = parse_netscape_cookies(cookies_raw)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ru-RU")

        # Подкладываем cookies до захода на страницу
        # Оставляем только seller.ozon.ru домен и родственные
        context.add_cookies([c for c in cookies if "ozon.ru" in c.get("domain", "")])

        page = context.new_page()

        captured = {"ok": False, "json": None, "status": None}

        def handle_response(resp):
            url = resp.url
            if API_SUBSTR in url:
                captured["status"] = resp.status
                try:
                    captured["json"] = resp.json()
                    captured["ok"] = True
                except Exception:
                    captured["ok"] = False

        page.on("response", handle_response)

        # Заходим на страницу, которая триггерит XHR
        page.goto(TARGET_PAGE, wait_until="domcontentloaded")

        # Ждём пока прилетит нужный XHR (макс 30 сек)
        page.wait_for_timeout(2000)
        page.wait_for_function(
            "() => true",
            timeout=30000
        )

        # Если не поймали — попробуем прямо дернуть XHR из страницы (через fetch браузера)
        if not captured["ok"]:
            try:
                data = page.evaluate(f"""
                    async () => {{
                      const r = await fetch("{API_SUBSTR}?__rr=3", {{
                        credentials: "include",
                        headers: {{
                          "accept": "application/json, text/plain, */*",
                          "x-o3-language": "ru",
                          "x-o3-app-name": "seller-ui"
                        }}
                      }});
                      return {{ status: r.status, text: await r.text() }};
                    }}
                
                captured["status"] = data["status"]
                try:
                    captured["json"] = json.loads(data["text"])
                    captured["ok"] = True
                except Exception:
                    captured["ok"] = False
            except Exception:
                captured["ok"] = False

        browser.close()

    if not captured["ok"]:
        raise SystemExit(f"Не удалось получить JSON. HTTP status={captured['status']}")

    # Достаём последние значения (берём первый/последний элемент — зависит от структуры, сделаем универсально)
    j = captured["json"]
    # Сохраняем JSON, который реально получили из XHR (не /tmp от curl)
    out_path = PROJECT / "last_delivery.json"
    out_path.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Saved JSON to:", out_path)
    if isinstance(j, dict):
    print("TOP KEYS:", list(j.keys())[:60])

    # Часто это {"dynamic-chart":[...]} или {"data":[...]} — ищем массив с объектами, где есть tariff
    def find_series(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, list) and v and isinstance(v[0], dict) and ("tariff" in v[0] or "averageDeliveryTime" in v[0]):
                    return v
                res = find_series(v)
                if res is not None:
                    return res
        elif isinstance(obj, list):
            for it in obj:
                res = find_series(it)
                if res is not None:
                    return res
        return None

    series = find_series(j)
    if not series:
        raise SystemExit("JSON получен, но не нашёл ряд с tariff/averageDeliveryTime")

    last = series[-1]
    tariff = last.get("tariff", {}) if isinstance(last, dict) else {}
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
