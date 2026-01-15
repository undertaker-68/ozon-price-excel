import json
from pathlib import Path
from playwright.sync_api import sync_playwright

PROJECT = Path(__file__).resolve().parent
COOKIES_TXT = PROJECT / "cookies.txt"

TARGET_PAGE = "https://seller.ozon.ru/app/analytics/sales-geography/local-packaging?__rr=3"
API_URL = "/api/site/seller-analytics/average-delivery-time/dynamic-chart?__rr=3"

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

def main():
    if not COOKIES_TXT.exists():
        raise SystemExit(f"Нет cookies файла: {COOKIES_TXT}")

    cookies = parse_netscape_cookies(
        COOKIES_TXT.read_text(encoding="utf-8", errors="ignore")
    )

    captured = {"ok": False, "json": None, "status": None}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ru-RU")

        context.add_cookies([c for c in cookies if "ozon.ru" in c["domain"]])

        page = context.new_page()

        def handle_response(resp):
            if "/average-delivery-time/dynamic-chart" in resp.url:
                captured["status"] = resp.status
                try:
                    captured["json"] = resp.json()
                    captured["ok"] = True
                except Exception:
                    pass

        page.on("response", handle_response)

        page.goto(TARGET_PAGE, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # fallback: fetch прямо из браузера
        if not captured["ok"]:
            data = page.evaluate("""
                async () => {
                  const r = await fetch("/api/site/seller-analytics/average-delivery-time/dynamic-chart?__rr=3", {
                    credentials: "include",
                    headers: {
                      "accept": "application/json, text/plain, */*",
                      "x-o3-language": "ru",
                      "x-o3-app-name": "seller-ui"
                    }
                  });
                  return { status: r.status, text: await r.text() };
                }
            """)
            captured["status"] = data["status"]
            try:
                captured["json"] = json.loads(data["text"])
                captured["ok"] = True
            except Exception:
                pass

        browser.close()

    if not captured["ok"]:
        raise SystemExit(f"Не удалось получить JSON, HTTP {captured['status']}")

    j = captured["json"]

    out = PROJECT / "last_delivery.json"
    out.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Saved JSON to:", out)

    # ищем массив с tariff
    def find_series(obj):
        if isinstance(obj, dict):
            for v in obj.values():
                r = find_series(v)
                if r:
                    return r
        elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
            if "tariff" in obj[0] or "averageDeliveryTime" in obj[0]:
                return obj
            for it in obj:
                r = find_series(it)
                if r:
                    return r
        return None

    series = find_series(j)
    if not series:
        raise SystemExit("JSON есть, но не найден ряд с tariff")

    last = series[-1]
    tariff = last.get("tariff", {})
    print("OK")
    print("date:", last.get("date"))
    print("averageDeliveryTime:", last.get("averageDeliveryTime"))
    print("tariffValue:", tariff.get("tariffValue"))
    print("fee:", tariff.get("fee"))

if __name__ == "__main__":
    main()
