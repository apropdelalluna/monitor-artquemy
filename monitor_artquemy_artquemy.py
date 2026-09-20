"""
Monitor de Ventas — Artquemy Gallery Barcelona (Playwright)
============================================================
Script independiente para monitorizar solo Artquemy.com
Usa Playwright para renderizar JavaScript y bypasear Cloudflare.

Requisitos:
    pip install playwright beautifulsoup4 schedule requests
    playwright install chromium

Variables de entorno (configurar en Render):
    GITHUB_TOKEN     → Token de GitHub con permisos repo
"""

import hashlib
import time
import logging
import json
import os
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/project/.playwright")
import re
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import schedule

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────

HORA_ENVIO        = "17:50"
ARCHIVO_ESTADO    = "estado_artquemy.json"
ARCHIVO_MENSUAL   = "ventas_mensuales_artquemy.json"
ARCHIVO_HISTORIAL = "historial_cambios_artquemy.json"
ARCHIVO_ARTISTAS  = "artistas_artquemy.json"

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "apropdelalluna/monitor-artquemy"
GITHUB_API   = "https://api.github.com"

PALABRAS_VENTA = [
    "sold", "vendido", "venut", "no disponible",
    "agotado", "reservado", "out of stock",
]

ARTISTAS = [
    {"nombre": "Alessia Innocenti",    "url": "https://artquemy.com/artists/alessia-innocenti/"},
    {"nombre": "Alessia Obinu",        "url": "https://artquemy.com/artists/alessia-obinu/"},
    {"nombre": "Catapop",              "url": "https://artquemy.com/artists/catapop/"},
    {"nombre": "Camil Escruela",       "url": "https://artquemy.com/artists/camil-escruela/"},
    {"nombre": "Carola Bagnato",       "url": "https://artquemy.com/artists/carola-bagnato/"},
    {"nombre": "Ceci SN",              "url": "https://artquemy.com/artists/ceci-sn/"},
    {"nombre": "Ciro Marra",           "url": "https://artquemy.com/artists/ciro-marra/"},
    {"nombre": "Collage Volage",       "url": "https://artquemy.com/artists/collage-volage/"},
    {"nombre": "Daniele Verzini",      "url": "https://artquemy.com/artists/daniele-verzini/"},
    {"nombre": "Droste Delacroix",     "url": "https://artquemy.com/artists/droste-de-la-croix/"},
    {"nombre": "Elina Cerla",          "url": "https://artquemy.com/artists/elina-cerla/"},
    {"nombre": "El Xupet Negre",       "url": "https://artquemy.com/artists/el-xupet-negre/"},
    {"nombre": "Evamik",               "url": "https://artquemy.com/artists/evamik/"},
    {"nombre": "Erwtje",               "url": "https://artquemy.com/artists/erwtje/"},
    {"nombre": "Irannis Mejias",       "url": "https://artquemy.com/artists/irannis/"},
    {"nombre": "Jamso",                "url": "https://artquemy.com/artists/jamso/"},
    {"nombre": "Jor Ros",              "url": "https://artquemy.com/artists/jor-ros/"},
    {"nombre": "Jorge Suñer",          "url": "https://artquemy.com/artists/jorge-suner/"},
    {"nombre": "Juandrés Vera",        "url": "https://artquemy.com/artists/juandres-vera/"},
    {"nombre": "Juncosa",              "url": "https://artquemy.com/artists/juncosa/"},
    {"nombre": "KST",                  "url": "https://artquemy.com/artists/kst/"},
    {"nombre": "Laura Gonballes",      "url": "https://artquemy.com/artists/laura-gonballes/"},
    {"nombre": "Laura Plou",           "url": "https://artquemy.com/artists/laura-plou/"},
    {"nombre": "Lourdes Villagómez",   "url": "https://artquemy.com/artists/lourdes-villagomez/"},
    {"nombre": "Luciana Zamarbide",    "url": "https://artquemy.com/artists/luciana-zamarbide/"},
    {"nombre": "Luz Marie Iturbe",     "url": "https://artquemy.com/artists/luz-marie-iturbe/"},
    {"nombre": "Manuel Enríquez",      "url": "https://artquemy.com/artists/manuel-enriquez/"},
    {"nombre": "Michelle Andrade",     "url": "https://artquemy.com/artists/michelle-andrade/"},
    {"nombre": "Món Mort",             "url": "https://artquemy.com/artists/mon-mort/"},
    {"nombre": "Okobé",                "url": "https://artquemy.com/artists/okobe/"},
    {"nombre": "Prëo",                 "url": "https://artquemy.com/artists/preo/"},
    {"nombre": "Qwert",                "url": "https://artquemy.com/artists/qwert/"},
    {"nombre": "Rich One",             "url": "https://artquemy.com/artists/rich-one/"},
    {"nombre": "Rocco Del Franco",     "url": "https://artquemy.com/artists/rocco-del-franco/"},
    {"nombre": "Rocío Iannone",        "url": "https://artquemy.com/artists/rocio-iannone/"},
    {"nombre": "Soy feo pero te amo",  "url": "https://artquemy.com/artists/soy-feo-pero-te-amo/"},
    {"nombre": "Surfia",               "url": "https://artquemy.com/artists/surfia/"},
    {"nombre": "Tiny",                 "url": "https://artquemy.com/artists/tiny/"},
    {"nombre": "Urban Flowers",        "url": "https://artquemy.com/artists/urban-flowers/"},
    {"nombre": "Yilov",                "url": "https://artquemy.com/artists/yilov/"},
    {"nombre": "Various",              "url": "https://artquemy.com/artists/various/"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("monitor_artquemy_artquemy.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

estado: dict = {}
cambios_del_dia: list = []


# ── Servidor HTTP mínimo para mantener Render activo ──

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass

def iniciar_servidor():
    server = HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 10000))), HealthHandler)
    logging.info("Servidor HTTP activo en puerto %s", os.environ.get("PORT", 10000))
    server.serve_forever()


# ── GitHub ──

def github_cargar_archivo(nombre: str) -> str | None:
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{nombre}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 200:
        import base64
        return base64.b64decode(resp.json()["content"]).decode("utf-8")
    return None

def github_guardar_archivo(nombre: str) -> bool:
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{nombre}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    import base64
    with open(nombre, "rb") as f:
        contenido_b64 = base64.b64encode(f.read()).decode()
    # Get SHA
    resp = requests.get(url, headers=headers, timeout=15)
    sha = resp.json().get("sha") if resp.status_code == 200 else None
    payload = {
        "message": f"Monitor: actualizar {nombre} [{datetime.now().strftime('%d/%m/%Y %H:%M')}] [skip ci]",
        "content": contenido_b64,
    }
    if sha:
        payload["sha"] = sha
    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    if resp.status_code in (200, 201):
        logging.info("✅ %s guardado en GitHub.", nombre)
        return True
    logging.error("Error guardando %s: %s", nombre, resp.text[:200])
    return False


# ── Extracción de obras con Playwright ──

_PW_SCRIPT = """
import sys, os
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/opt/render/project/.playwright"
from playwright.sync_api import sync_playwright
url = sys.argv[1]
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-gpu","--single-process"])
        ctx = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36", locale="es-ES")
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        try:
            page.wait_for_selector("li.product", timeout=6000)
        except Exception:
            pass
        page.wait_for_timeout(500)
        print(page.content())
        browser.close()
except Exception as e:
    import sys; sys.stderr.write(str(e)); sys.exit(1)
"""

_PW_SCRIPT_PATH = "/tmp/_pw_fetch_artquemy.py"

def _init_pw_script():
    with open(_PW_SCRIPT_PATH, "w") as f:
        f.write(_PW_SCRIPT)

def obtener_html_playwright(url: str) -> str | None:
    """Ejecuta Playwright en subproceso con timeout duro de 30s."""
    import subprocess
    try:
        result = subprocess.run(
            ["python3", _PW_SCRIPT_PATH, url],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
        logging.warning("Playwright sin resultado para %s: %s", url, result.stderr[:200])
        return None
    except subprocess.TimeoutExpired:
        logging.warning("Timeout duro (30s) en %s — saltando", url)
        return None
    except Exception as e:
        logging.error("Error subproceso Playwright en %s: %s", url, e)
        return None


def precio_a_numero(precio_str: str) -> float:
    if not precio_str:
        return 0.0
    try:
        s = re.sub(r"[^\d,.]", "", precio_str.replace("\xa0", ""))
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def extraer_obras(soup: BeautifulSoup) -> dict:
    obras = {}

    SELECTORES_PRODUCTO = [
        "li.product",
        "ul.products li",
        ".jupiterx-product-container",
        "article.product",
        ".wc-block-grid__product",
        ".product-item",
    ]

    productos = []
    for sel in SELECTORES_PRODUCTO:
        productos = soup.select(sel)
        if productos:
            break

    if not productos:
        logging.warning("No se encontraron productos con ningún selector conocido")
        return obras

    for producto in productos:
        titulo_el = (
            producto.select_one(".woocommerce-loop-product__title")
            or producto.select_one("h2")
            or producto.select_one("h3")
        )
        titulo = titulo_el.get_text(strip=True) if titulo_el else ""
        if not titulo:
            continue

        enlace_el = producto.select_one("a.woocommerce-LoopProduct-link, a.woocommerce-loop-product__link, a")
        url_obra = enlace_el.get("href", "") if enlace_el else ""

        precio_el = producto.select_one(".price")
        precio_str_raw = precio_el.get_text(strip=True) if precio_el else ""

        clases = " ".join(producto.get("class", []))
        palabras_texto = producto.get_text().lower()
        es_vendido = (
            "outofstock" in clases
            or any(p in palabras_texto for p in PALABRAS_VENTA)
            or "sold" in titulo.lower()
        )

        if es_vendido:
            estado_obra = "vendido"
        else:
            estado_obra = "disponible"

        precio_num = precio_a_numero(precio_str_raw)

        obras[url_obra] = {
            "titulo": titulo,
            "precio": precio_str_raw,
            "precio_num": precio_num,
            "estado": estado_obra,
            "url": url_obra,
        }

    return obras


def obtener_contenido(artista: dict) -> dict | None:
    try:
        url = artista["url"]
        obras_totales = {}
        textos = []
        pagina = 1
        max_paginas = 10

        while url and pagina <= max_paginas:
            html = obtener_html_playwright(url)
            if not html:
                logging.warning("Sin HTML para %s", artista["nombre"])
                break

            soup = BeautifulSoup(html, "html.parser")
            zona = soup.select_one(".products") or soup.select_one("main") or soup.body
            textos.append(zona.get_text(separator="\n", strip=True) if zona else "")

            obras_pagina = extraer_obras(soup)
            obras_totales.update(obras_pagina)

            siguiente = soup.select_one("a.next.page-numbers, .woocommerce-pagination a.next")
            url = siguiente["href"] if siguiente else None
            pagina += 1

        texto_completo = "\n".join(textos)
        hash_actual = hashlib.md5(texto_completo.encode()).hexdigest()
        return {"texto": texto_completo, "hash": hash_actual, "obras": obras_totales}

    except Exception as e:
        logging.error("Error en %s: %s", artista["nombre"], e)
        return None


# ── Estado y persistencia ──

def cargar_estado() -> None:
    global estado
    contenido = github_cargar_archivo(ARCHIVO_ESTADO)
    if contenido:
        try:
            estado = json.loads(contenido)
            logging.info("Estado cargado desde GitHub: %d artistas.", len(estado))
        except Exception as e:
            logging.warning("No se pudo parsear estado: %s", e)
    else:
        logging.info("Sin estado previo — primer escaneo.")


def guardar_estado() -> None:
    try:
        with open(ARCHIVO_ESTADO, "w", encoding="utf-8") as f:
            json.dump(estado, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_ESTADO)
    except Exception as e:
        logging.error("Error guardando estado: %s", e)


def detectar_cambios_obras(obras_nuevas: dict, obras_viejas: dict, artista_nombre: str = "") -> list:
    cambios = []
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")

    for url, info_nueva in obras_nuevas.items():
        info_vieja = obras_viejas.get(url)
        if info_vieja is None:
            if info_nueva["estado"] == "vendido":
                cambios.append({"tipo": "nueva_vendida", "titulo": info_nueva["titulo"], "artista": artista_nombre,
                                 "precio": info_nueva["precio"], "precio_num": info_nueva["precio_num"], "url": url, "fecha": fecha})
            else:
                cambios.append({"tipo": "nueva", "titulo": info_nueva["titulo"], "artista": artista_nombre,
                                 "precio": info_nueva["precio"], "precio_num": info_nueva["precio_num"], "url": url, "fecha": fecha})
        else:
            if info_vieja["estado"] != info_nueva["estado"]:
                if info_nueva["estado"] == "vendido":
                    cambios.append({"tipo": "vendida", "titulo": info_vieja["titulo"], "artista": artista_nombre,
                                     "precio": info_vieja["precio"], "precio_num": info_vieja["precio_num"], "url": url, "fecha": fecha})
                elif info_vieja["estado"] == "vendido":
                    cambios.append({"tipo": "nueva", "titulo": info_nueva["titulo"], "artista": artista_nombre,
                                     "precio": info_nueva["precio"], "precio_num": info_nueva["precio_num"], "url": url, "fecha": fecha})
            elif (info_vieja["estado"] == "disponible" and info_nueva["estado"] == "disponible"
                  and info_vieja.get("precio_num", 0) != info_nueva.get("precio_num", 0)
                  and info_nueva.get("precio_num", 0) > 0 and info_vieja.get("precio_num", 0) > 0):
                cambios.append({"tipo": "precio_cambiado", "titulo": info_nueva["titulo"], "artista": artista_nombre,
                                 "precio": info_nueva["precio"], "precio_num": info_nueva["precio_num"],
                                 "precio_anterior": info_vieja["precio"], "url": url, "fecha": fecha})

    for url, info_vieja in obras_viejas.items():
        if url not in obras_nuevas:
            cambios.append({"tipo": "desaparecida", "titulo": info_vieja["titulo"], "artista": artista_nombre,
                             "precio": info_vieja["precio"], "precio_num": info_vieja["precio_num"], "url": url, "fecha": fecha})

    return cambios


def guardar_ventas_mensuales(cambios: list) -> None:
    try:
        contenido = github_cargar_archivo(ARCHIVO_MENSUAL)
        acumulado = json.loads(contenido) if contenido else {}
        mes_actual = datetime.now().strftime("%Y-%m")
        if mes_actual not in acumulado:
            acumulado[mes_actual] = []

        existentes_url = set((e.get("url", ""), e.get("fecha", "")[:10]) for e in acumulado[mes_actual] if e.get("url"))

        for cambio in cambios:
            artista = cambio["artista"]
            for c in cambio.get("cambios_obras", []):
                if c["tipo"] in ("vendida", "nueva_vendida") and c.get("precio_num", 0) >= 0:
                    url_obra = c.get("url", "")
                    fecha_dia = datetime.now().strftime("%d/%m/%Y")
                    if url_obra and (url_obra, fecha_dia) in existentes_url:
                        continue
                    if url_obra:
                        existentes_url.add((url_obra, fecha_dia))
                    acumulado[mes_actual].append({
                        "fecha": datetime.now().strftime("%d/%m/%Y %H:%M"),
                        "artista": artista,
                        "obra": c["titulo"],
                        "precio": c["precio"],
                        "precio_num": c.get("precio_num", 0.0),
                        "tipo": c["tipo"],
                        "url": url_obra,
                    })

        with open(ARCHIVO_MENSUAL, "w", encoding="utf-8") as f:
            json.dump(acumulado, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_MENSUAL)
    except Exception as e:
        logging.error("Error guardando ventas mensuales: %s", e)


def guardar_historial(cambios_planos: list) -> None:
    try:
        contenido = github_cargar_archivo(ARCHIVO_HISTORIAL)
        historial = json.loads(contenido) if contenido else []
        historial.extend(cambios_planos)
        historial = historial[-2000:]
        with open(ARCHIVO_HISTORIAL, "w", encoding="utf-8") as f:
            json.dump(historial, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_HISTORIAL)
    except Exception as e:
        logging.error("Error guardando historial: %s", e)


def cargar_artistas_github() -> None:
    global ARTISTAS
    contenido = github_cargar_archivo(ARCHIVO_ARTISTAS)
    if contenido:
        try:
            ARTISTAS = json.loads(contenido)
            logging.info("Artistas vigilados : %d", len(ARTISTAS))
        except Exception:
            logging.warning("No se pudo parsear artistas_artquemy.json — usando lista por defecto")


# ── Comprobación principal ──

def comprobar_todos() -> None:
    global estado, cambios_del_dia
    logging.info("=" * 54)
    logging.info("Inicio comprobación — %s", datetime.now().strftime("%d/%m/%Y %H:%M"))
    logging.info("Artistas a comprobar: %d", len(ARTISTAS))

    cambios_del_dia = []
    nuevo_estado = {}

    for artista in ARTISTAS:
        logging.info("Comprobando: %s", artista["nombre"])
        datos = obtener_contenido(artista)

        if datos is None:
            logging.warning("Sin datos para %s — manteniendo estado anterior", artista["nombre"])
            if artista["nombre"] in estado:
                nuevo_estado[artista["nombre"]] = estado[artista["nombre"]]
            continue

        estado_artista_viejo = estado.get(artista["nombre"], {})
        obras_viejas = estado_artista_viejo.get("obras", {})
        obras_nuevas = datos["obras"]

        cambios_obras = detectar_cambios_obras(obras_nuevas, obras_viejas, artista["nombre"])

        nuevo_estado[artista["nombre"]] = {
            "url": artista["url"],
            "hash": datos["hash"],
            "obras": obras_nuevas,
            "ultima_comprobacion": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }

        if cambios_obras:
            logging.info("  %d cambios en %s", len(cambios_obras), artista["nombre"])
            for c in cambios_obras:
                logging.info("    [%s] %s — %s", c["tipo"], c["titulo"], c["precio"])
            cambios_del_dia.append({"artista": artista, "cambios_obras": cambios_obras})

        time.sleep(3)  # Pausa entre artistas para no sobrecargar

    estado = nuevo_estado
    guardar_estado()

    if cambios_del_dia:
        cambios_planos = []
        for c in cambios_del_dia:
            for obra in c["cambios_obras"]:
                cambios_planos.append(obra)
        guardar_ventas_mensuales(cambios_del_dia)
        guardar_historial(cambios_planos)
        logging.info("Comprobación finalizada — %d cambios.", len(cambios_planos))
    else:
        logging.info("Comprobación finalizada — Sin cambios detectados.")


# ── Main ──

def main() -> None:
    logging.info("=" * 54)
    logging.info("Monitor Artquemy (Playwright) iniciado")
    logging.info("Artistas vigilados : %d", len(ARTISTAS))
    logging.info("=" * 54)

    threading.Thread(target=iniciar_servidor, daemon=True).start()

    _init_pw_script()
    cargar_artistas_github()
    cargar_estado()

    comprobar_todos()

    schedule.every().day.at(HORA_ENVIO).do(comprobar_todos)

    logging.info("Scheduler activo. Comprobación automática a las %s UTC.", HORA_ENVIO)
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
