"""Script auxiliar — ejecutado por monitor_artquemy_artquemy.py en subproceso."""
import sys
import os
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/.playwright")

from playwright.sync_api import sync_playwright

url = sys.argv[1]
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            locale="es-ES",
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        try:
            page.wait_for_selector("li.product", timeout=6000)
        except Exception:
            pass
        page.wait_for_timeout(500)
        html = page.content()
        browser.close()
        print(html)
except Exception as e:
    sys.stderr.write(str(e))
    sys.exit(1)
