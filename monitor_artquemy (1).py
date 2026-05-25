"""
Monitor de Ventas — Artquemy Gallery Barcelona
===============================================
Vigila las páginas de todos los artistas de artquemy.com
y detecta cambios: obras nuevas, vendidas, cambios de precio
y artistas nuevos o eliminados.

Misma arquitectura que monitor_artevistas.py:
  - Scraping con BeautifulSoup
  - Persistencia en GitHub (JSON)
  - Servidor HTTP mínimo para Render
  - Schedule para comprobación automática a las 17:50

Requisitos:
    pip install requests beautifulsoup4 schedule

Variables de entorno (configurar en Render):
    GITHUB_TOKEN     → Token de GitHub con permisos repo
"""

import hashlib
import time
import difflib
import logging
import json
import os
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

HORA_ENVIO       = "17:50"
ARCHIVO_ESTADO   = "estado_artquemy.json"
ARCHIVO_MENSUAL  = "ventas_mensuales_artquemy.json"
ARCHIVO_HISTORIAL = "historial_cambios_artquemy.json"
ARCHIVO_ARTISTAS = "artistas_artquemy.json"

# GitHub
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "apropdelalluna/monitor-artquemy"   # <-- cambia si el repo tiene otro nombre
GITHUB_API   = "https://api.github.com"

PALABRAS_VENTA = [
    "sold", "vendido", "venut", "no disponible",
    "agotado", "reservado", "out of stock",
]

# Lista inicial de artistas — se actualiza automáticamente desde la web
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

# ─────────────────────────────────────────────
#  FIN DE CONFIGURACIÓN
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("monitor_artquemy.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

estado: dict = {}
cambios_del_dia: list = []


# ── Servidor HTTP mínimo para mantener Render activo ──

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/robots.txt":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"User-agent: *\nDisallow: /\n")
        elif self.path == "/":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

def iniciar_servidor_http():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logging.info("Servidor HTTP activo en puerto %d", port)
    server.serve_forever()









# ── Persistencia GitHub ───────────────────────

def github_guardar_archivo(nombre_archivo: str) -> None:
    if not GITHUB_TOKEN:
        logging.warning("GITHUB_TOKEN no configurado, no se guarda en GitHub.")
        return
    try:
        with open(nombre_archivo, "r", encoding="utf-8") as f:
            contenido = f.read()

        import base64
        contenido_b64 = base64.b64encode(contenido.encode("utf-8")).decode("utf-8")

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{nombre_archivo}"

        sha = None
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")

        payload = {
            "message": f"Monitor: actualizar {nombre_archivo} [{datetime.now().strftime('%d/%m/%Y %H:%M')}] [skip ci]",
            "content": contenido_b64,
        }
        if sha:
            payload["sha"] = sha

        r = requests.put(url, headers=headers, json=payload, timeout=15)
        if r.status_code in (200, 201):
            logging.info("✅ %s guardado en GitHub.", nombre_archivo)
        else:
            logging.error("Error guardando en GitHub: %s %s", r.status_code, r.text[:200])

    except Exception as e:
        logging.error("Error en github_guardar_archivo: %s", e)


def github_cargar_archivo(nombre_archivo: str) -> str | None:
    if not GITHUB_TOKEN:
        return None
    try:
        import base64
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{nombre_archivo}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return base64.b64decode(r.json()["content"]).decode("utf-8")
    except Exception as e:
        logging.warning("No se pudo cargar %s desde GitHub: %s", nombre_archivo, e)
    return None


# ── Estado ────────────────────────────────────

def cargar_estado() -> None:
    global estado
    # Intentar primero desde GitHub
    contenido = github_cargar_archivo(ARCHIVO_ESTADO)
    if contenido:
        try:
            estado = json.loads(contenido)
            logging.info("Estado cargado desde GitHub: %d artistas.", len(estado))
            # Guardar localmente para uso offline
            with open(ARCHIVO_ESTADO, "w", encoding="utf-8") as f:
                f.write(contenido)
            return
        except json.JSONDecodeError:
            pass
    # Fallback a archivo local
    if os.path.exists(ARCHIVO_ESTADO):
        try:
            with open(ARCHIVO_ESTADO, "r", encoding="utf-8") as f:
                estado = json.load(f)
            logging.info("Estado cargado localmente: %d artistas.", len(estado))
        except (json.JSONDecodeError, OSError) as e:
            logging.warning("No se pudo cargar el estado: %s", e)
            estado = {}


def guardar_estado() -> None:
    try:
        with open(ARCHIVO_ESTADO, "w", encoding="utf-8") as f:
            json.dump(estado, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_ESTADO)
        meta = {"ultima_comprobacion": datetime.now().strftime("%d/%m/%Y %H:%M")}
        with open("meta_artquemy.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
        github_guardar_archivo("meta_artquemy.json")
    except OSError as e:
        logging.error("No se pudo guardar el estado: %s", e)


def guardar_ventas_mensuales(cambios: list) -> None:
    try:
        acumulado = {}
        contenido = github_cargar_archivo(ARCHIVO_MENSUAL)
        if contenido:
            acumulado = json.loads(contenido)
        elif os.path.exists(ARCHIVO_MENSUAL):
            with open(ARCHIVO_MENSUAL, "r", encoding="utf-8") as f:
                acumulado = json.load(f)

        mes_actual = datetime.now().strftime("%Y-%m")
        if mes_actual not in acumulado:
            acumulado[mes_actual] = []

        for cambio in cambios:
            artista = cambio["artista"]["nombre"]
            for c in cambio.get("cambios_obras", []):
                if c["tipo"] in ("vendida", "nueva_vendida") and c.get("precio_num", 0) >= 0:
                    acumulado[mes_actual].append({
                        "fecha":      datetime.now().strftime("%d/%m/%Y %H:%M"),
                        "artista":    artista,
                        "obra":       c["titulo"],
                        "precio":     c["precio"],
                        "precio_num": c.get("precio_num", 0.0),
                        "tipo":       c["tipo"],
                        "url":        c.get("url", ""),
                    })

        with open(ARCHIVO_MENSUAL, "w", encoding="utf-8") as f:
            json.dump(acumulado, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_MENSUAL)
    except Exception as e:
        logging.error("Error guardando ventas mensuales: %s", e)


def guardar_historial(cambios: list) -> None:
    try:
        historial = []
        contenido = github_cargar_archivo(ARCHIVO_HISTORIAL)
        if contenido:
            historial = json.loads(contenido)
        elif os.path.exists(ARCHIVO_HISTORIAL):
            with open(ARCHIVO_HISTORIAL, "r", encoding="utf-8") as f:
                historial = json.load(f)

        for cambio in cambios:
            historial.append({
                "fecha":        datetime.now().strftime("%d/%m/%Y %H:%M"),
                "artista":      cambio["artista"]["nombre"],
                "cambios_obras": cambio.get("cambios_obras", []),
            })

        # Mantener solo los últimos 1000 registros
        historial = historial[-1000:]

        with open(ARCHIVO_HISTORIAL, "w", encoding="utf-8") as f:
            json.dump(historial, f, ensure_ascii=False, indent=2)
        github_guardar_archivo(ARCHIVO_HISTORIAL)
    except Exception as e:
        logging.error("Error guardando historial: %s", e)


# ── Scraping ──────────────────────────────────

def precio_a_numero(precio_str: str) -> float:
    try:
        limpio = re.sub(r"[^\d.,]", "", precio_str)
        if "," in limpio and "." in limpio:
            limpio = limpio.replace(".", "").replace(",", ".")
        elif "," in limpio:
            limpio = limpio.replace(",", ".")
        return float(limpio)
    except Exception:
        return 0.0


def obtener_precio_desde_producto(url_producto: str) -> tuple[str, float]:
    """Intenta extraer precio de la página individual vía JSON-LD."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }
    try:
        resp = requests.get(url_producto, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                productos = []
                if isinstance(data, dict):
                    if data.get("@type") == "Product":
                        productos.append(data)
                    elif "@graph" in data:
                        productos = [x for x in data["@graph"] if x.get("@type") == "Product"]
                for prod in productos:
                    offers = prod.get("offers", {})
                    if isinstance(offers, dict):
                        precio_num = float(offers.get("price", 0))
                        if precio_num > 0:
                            moneda = offers.get("priceCurrency", "EUR")
                            precio_str = f"{precio_num:,.2f}€".replace(",", "X").replace(".", ",").replace("X", ".")
                            return precio_str, precio_num
                    for spec in (offers if isinstance(offers, list) else offers.get("priceSpecification", [])):
                        precio_num = float(spec.get("price", 0))
                        if precio_num > 0:
                            precio_str = f"{precio_num:,.2f}€".replace(",", "X").replace(".", ",").replace("X", ".")
                            return precio_str, precio_num
            except Exception:
                continue
    except Exception as e:
        logging.debug("No se pudo obtener precio de %s: %s", url_producto, e)

    return "Precio no disponible", 0.0


def extraer_obras(soup: BeautifulSoup) -> dict:
    """Extrae obras de la página de un artista en Artquemy."""
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
            logging.debug("Selector '%s' encontró %d productos", sel, len(productos))
            break

    if not productos:
        logging.warning("No se encontraron productos con ningún selector conocido")
        return obras

    for producto in productos:
        # Título
        titulo_el = (
            producto.select_one(".woocommerce-loop-product__title")
            or producto.select_one(".raven-product-item-content")
            or producto.select_one("h2")
            or producto.select_one("h3")
            or producto.select_one(".product-title")
        )
        titulo = titulo_el.get_text(strip=True) if titulo_el else "Sin título"
        if not titulo or titulo == "Sin título":
            continue

        # Precio — en Artquemy el precio desaparece al venderse
        precio_el = (
            producto.select_one(".woocommerce-Price-amount")
            or producto.select_one(".price")
            or producto.select_one("[class*='price']")
        )
        precio_str = precio_el.get_text(strip=True) if precio_el else "Precio no disponible"
        precio_str = precio_str.split("–")[0].split("-")[0].strip()
        precio_num = precio_a_numero(precio_str)

        # Estado vendido — Artquemy usa clase jupiterx-out-of-stock y texto "SOLD"
        sold_el = producto.select_one(
            ".jupiterx-out-of-stock, .out-of-stock, .sold_out_badge, "
            "[class*='sold'], [class*='outofstock']"
        )
        texto_producto = producto.get_text(separator=" ", strip=True).lower()
        es_vendido = bool(sold_el) or any(p in texto_producto for p in PALABRAS_VENTA)

        # Si el precio es "SOLD" literalmente también es vendido
        if precio_str.upper() == "SOLD":
            es_vendido = True
            precio_str = "Precio no disponible"
            precio_num = 0.0

        estado_obra = "vendido" if es_vendido else "disponible"

        # URL de la obra
        url_obra = None
        enlace = producto.select_one(
            "a.woocommerce-LoopProduct-link, "
            "a.woocommerce-loop-product__link, "
            ".jupiterx-product-container > a"
        )
        if enlace and enlace.get("href"):
            url_obra = enlace["href"]

        clave = url_obra if url_obra else titulo

        obras[clave] = {
            "titulo":     titulo,
            "precio":     precio_str,
            "precio_num": precio_num,
            "estado":     estado_obra,
            "url":        url_obra,
        }

    return obras


def obtener_contenido(artista: dict) -> dict | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }
    try:
        obras_totales = {}
        textos = []
        url = artista["url"]
        pagina = 1
        max_paginas = 10

        while url and pagina <= max_paginas:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            zona = soup.select_one(".products") or soup.select_one("main") or soup.body
            textos.append(zona.get_text(separator="\n", strip=True))

            obras_pagina = extraer_obras(soup)
            obras_totales.update(obras_pagina)

            siguiente = soup.select_one("a.next.page-numbers, .woocommerce-pagination a.next")
            url = siguiente["href"] if siguiente else None
            pagina += 1

        if pagina > 2:
            logging.info("[%s] Paginación: %d páginas, %d obras totales",
                         artista["nombre"], pagina - 1, len(obras_totales))

        texto_completo = "\n".join(textos)
        hash_actual = hashlib.md5(texto_completo.encode()).hexdigest()

        return {"texto": texto_completo, "hash": hash_actual, "obras": obras_totales}

    except requests.RequestException as e:
        logging.error("[%s] Error: %s", artista["nombre"], e)
        return None


# ── Análisis de cambios ───────────────────────

def investigar_obra_desaparecida(info_vieja: dict, titulo: str) -> tuple[str, str, float]:
    """
    Investiga una obra que ha desaparecido del listado del artista.
    Visita su URL individual para determinar si fue vendida, retirada o eliminada.
    Devuelve (tipo, precio_str, precio_num).
    Tipos posibles: 'vendida', 'retirada', 'eliminada', 'desaparecida'
    """
    url = info_vieja.get("url")
    precio_guardado = info_vieja.get("precio", "Precio no disponible")
    precio_num_guardado = info_vieja.get("precio_num", 0.0)

    if not url:
        return "desaparecida", precio_guardado, precio_num_guardado

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)

        # 404 — eliminada definitivamente
        if resp.status_code == 404:
            logging.info("  🗑️  Obra eliminada (404): %s", titulo)
            return "eliminada", precio_guardado, precio_num_guardado

        # 200 — la página existe pero no aparece en el listado del artista
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            texto = soup.get_text(separator=" ", strip=True).lower()

            # Buscar indicadores de venta en la página individual
            sold_el = soup.select_one(
                ".jupiterx-out-of-stock, .out-of-stock, .out-of-stock-label, "
                "[class*='sold'], [class*='outofstock']"
            )
            precio_el = soup.select_one(".woocommerce-Price-amount, .price")
            precio_texto = precio_el.get_text(strip=True).upper() if precio_el else ""

            es_vendida = (
                bool(sold_el)
                or any(p in texto for p in PALABRAS_VENTA)
                or precio_texto == "SOLD"
            )

            if es_vendida:
                logging.info("  🔴 Obra vendida (desapareció del listado): %s — %s",
                             titulo, precio_guardado)
                return "vendida", precio_guardado, precio_num_guardado
            else:
                logging.info("  ⚠️  Obra retirada temporalmente: %s", titulo)
                return "retirada", precio_guardado, precio_num_guardado

    except requests.RequestException as e:
        logging.debug("No se pudo verificar obra desaparecida '%s': %s", titulo, e)

    return "desaparecida", precio_guardado, precio_num_guardado


def detectar_cambios_obras(obras_nuevas: dict, obras_viejas: dict) -> list:
    cambios = []
    obras_nuevas_por_titulo = {}
    for clave_n, info_n in obras_nuevas.items():
        t = info_n.get("titulo", clave_n)
        if t not in obras_nuevas_por_titulo:
            obras_nuevas_por_titulo[t] = (clave_n, info_n)

    for clave, info_vieja in obras_viejas.items():
        titulo = info_vieja.get("titulo", clave)
        if clave not in obras_nuevas:
            match = obras_nuevas_por_titulo.get(titulo)
            if match:
                _, info_nueva = match
                if info_vieja["estado"] != info_nueva["estado"]:
                    if info_nueva["estado"] == "vendido":
                        # Recuperar precio del estado anterior
                        precio = info_vieja["precio"]
                        precio_num = info_vieja.get("precio_num", 0.0)
                        cambios.append({
                            "tipo": "vendida", "titulo": titulo,
                            "precio": precio, "precio_num": precio_num,
                            "url": info_nueva.get("url", "")
                        })
                    elif info_vieja["estado"] == "vendido" and info_nueva["estado"] == "disponible":
                        cambios.append({
                            "tipo": "nueva", "titulo": titulo,
                            "precio": info_nueva["precio"],
                            "precio_num": info_nueva.get("precio_num", 0.0),
                            "url": info_nueva.get("url", "")
                        })
            else:
                # La obra no aparece en el listado — investigar qué pasó
                tipo_real, precio_real, precio_num_real = investigar_obra_desaparecida(
                    info_vieja, titulo
                )
                cambios.append({
                    "tipo": tipo_real,
                    "titulo": titulo,
                    "precio": precio_real,
                    "precio_num": precio_num_real,
                    "url": info_vieja.get("url", "")
                })
        else:
            info_nueva = obras_nuevas[clave]
            if info_vieja["estado"] != info_nueva["estado"]:
                if info_nueva["estado"] == "vendido":
                    # Precio viene del estado anterior (antes de venderse)
                    precio = info_vieja["precio"]
                    precio_num = info_vieja.get("precio_num", 0.0)
                    cambios.append({
                        "tipo": "vendida", "titulo": titulo,
                        "precio": precio, "precio_num": precio_num,
                        "url": info_nueva.get("url", "")
                    })
                elif info_vieja["estado"] == "vendido" and info_nueva["estado"] == "disponible":
                    cambios.append({
                        "tipo": "nueva", "titulo": titulo,
                        "precio": info_nueva["precio"],
                        "precio_num": info_nueva.get("precio_num", 0.0),
                        "url": info_nueva.get("url", "")
                    })
            # Detectar cambio de precio (obra disponible con precio distinto)
            elif (info_vieja["estado"] == "disponible"
                  and info_nueva["estado"] == "disponible"
                  and info_vieja.get("precio_num", 0) != info_nueva.get("precio_num", 0)
                  and info_nueva.get("precio_num", 0) > 0):
                cambios.append({
                    "tipo": "precio_cambiado", "titulo": titulo,
                    "precio": info_nueva["precio"],
                    "precio_num": info_nueva.get("precio_num", 0.0),
                    "precio_anterior": info_vieja["precio"],
                    "url": info_nueva.get("url", "")
                })

    claves_viejas_titulos = {info.get("titulo", clave) for clave, info in obras_viejas.items()}

    for clave, info_nueva in obras_nuevas.items():
        titulo = info_nueva.get("titulo", clave)
        if clave not in obras_viejas and titulo not in claves_viejas_titulos:
            tipo = "nueva_vendida" if info_nueva.get("estado") == "vendido" else "nueva"
            precio_str = info_nueva["precio"]
            precio_num = info_nueva.get("precio_num", 0.0)
            # Artquemy oculta el precio al vender — no hay forma de recuperarlo
            if tipo == "nueva_vendida":
                precio_str = "Precio no disponible"
                precio_num = 0.0
            cambios.append({
                "tipo": tipo, "titulo": titulo,
                "precio": precio_str, "precio_num": precio_num,
                "url": info_nueva.get("url", "")
            })

    return cambios


def generar_diff(texto_viejo: str, texto_nuevo: str) -> str:
    diff = list(difflib.unified_diff(
        texto_viejo.splitlines(),
        texto_nuevo.splitlines(),
        lineterm="", n=2,
    ))
    if not diff:
        return "(sin diferencias)"
    resumen = "\n".join(diff[:60])
    if len(diff) > 60:
        resumen += f"\n... y {len(diff) - 60} líneas más."
    return resumen


# ── Comprobación principal ────────────────────

def comprobar_artista(artista: dict) -> dict | None:
    global estado, cambios_del_dia

    nombre = artista["nombre"]
    logging.info("Comprobando: %s", nombre)

    datos = obtener_contenido(artista)
    if datos is None:
        return None

    hash_actual = datos["hash"]
    obras_actuales = datos["obras"]
    texto_actual = datos["texto"]

    entrada_vieja = estado.get(nombre, {})
    hash_viejo = entrada_vieja.get("hash", "")
    obras_viejas = entrada_vieja.get("obras", {})
    texto_viejo = entrada_vieja.get("texto", "")

    cambios_obras = detectar_cambios_obras(obras_actuales, obras_viejas)

    if hash_actual != hash_viejo or cambios_obras:
        diff = generar_diff(texto_viejo, texto_actual)
        hora = datetime.now().strftime("%H:%M")
        logging.info("  → Cambios detectados en %s: %d", nombre, len(cambios_obras))
        for c in cambios_obras:
            logging.info("    [%s] %s — %s", c["tipo"], c["titulo"], c["precio"])

        cambio = {
            "artista":      artista,
            "hora":         hora,
            "cambios_obras": cambios_obras,
            "diff":         diff,
        }
        cambios_del_dia.append(cambio)

        # Actualizar estado
        estado[nombre] = {
            "hash":  hash_actual,
            "texto": texto_actual,
            "obras": obras_actuales,
        }
        return cambio
    else:
        # Sin cambios — actualizar igual para refrescar obras
        estado[nombre] = {
            "hash":  hash_actual,
            "texto": texto_actual,
            "obras": obras_actuales,
        }
        return None


def comprobar_todos() -> None:
    global cambios_del_dia
    logging.info("=" * 50)
    logging.info("Inicio comprobación — %s", datetime.now().strftime("%d/%m/%Y %H:%M"))
    logging.info("Artistas a comprobar: %d", len(ARTISTAS))

    cambios_del_dia = []

    for artista in ARTISTAS:
        comprobar_artista(artista)
        time.sleep(2)  # pausa cortés entre peticiones

    guardar_estado()

    if cambios_del_dia:
        guardar_ventas_mensuales(cambios_del_dia)
        guardar_historial(cambios_del_dia)
        # enviar_resumen_cambios(cambios_del_dia)  # emails desactivados
        logging.info("Comprobación finalizada — %d artistas con cambios.", len(cambios_del_dia))
    else:
        logging.info("Comprobación finalizada — Sin cambios detectados.")


# ── Detección de artistas nuevos ──────────────

def obtener_artistas_web() -> list:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-ES,es;q=0.9",
    }
    try:
        r = requests.get("https://artquemy.com/artists/", headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        artistas = []
        for a in soup.select("a[href*='/artists/']"):
            href = a.get("href", "")
            nombre = a.get_text(strip=True)
            if not href or not nombre:
                continue
            if not href.startswith("http"):
                href = "https://artquemy.com" + href
            if not href.endswith("/"):
                href += "/"
            if href.rstrip("/").endswith("/artists"):
                continue
            if len(nombre) < 2:
                continue
            artistas.append({"nombre": nombre, "url": href})
        vistos = set()
        unicos = []
        for a in artistas:
            if a["url"] not in vistos:
                vistos.add(a["url"])
                unicos.append(a)
        return unicos
    except Exception as e:
        logging.error("Error obteniendo artistas de la web: %s", e)
        return []


# URLs de artistas a ignorar (páginas rotas o inaccesibles)
URLS_IGNORAR = {
    "https://artquemy.com/artists/ana-rosenzweig/",
}

def detectar_artistas_nuevos() -> list:
    global ARTISTAS
    artistas_web = obtener_artistas_web()
    if not artistas_web:
        return []

    # Filtrar URLs problemáticas
    artistas_web = [a for a in artistas_web if a["url"] not in URLS_IGNORAR]

    urls_actuales = {a["url"] for a in ARTISTAS}
    urls_web = {a["url"] for a in artistas_web}

    nuevos = [a for a in artistas_web if a["url"] not in urls_actuales]
    desaparecidos = [a for a in ARTISTAS if a["url"] not in urls_web]

    cambios = []

    for a in nuevos:
        logging.info("🟢 Nuevo artista: %s (%s)", a["nombre"], a["url"])
        ARTISTAS.append(a)
        cambios.append({"tipo": "nuevo_artista", "artista": a})

    for a in desaparecidos:
        try:
            r = requests.head(a["url"], timeout=10)
            if r.status_code == 404:
                logging.info("🔴 Artista eliminado (404): %s", a["nombre"])
                ARTISTAS = [x for x in ARTISTAS if x["url"] != a["url"]]
                cambios.append({"tipo": "artista_eliminado", "artista": a})
        except Exception:
            pass

    return cambios


def cargar_artistas_github() -> None:
    global ARTISTAS
    contenido = github_cargar_archivo(ARCHIVO_ARTISTAS)
    if contenido:
        try:
            ARTISTAS = json.loads(contenido)
            logging.info("Lista de artistas cargada desde GitHub: %d artistas.", len(ARTISTAS))
        except Exception as e:
            logging.warning("No se pudo parsear lista de artistas: %s", e)


# ── Email ─────────────────────────────────────


# ── Main ──────────────────────────────────────

def main() -> None:
    logging.info("=" * 55)
    logging.info("Monitor Artquemy iniciado")
    logging.info("Artistas vigilados : %d", len(ARTISTAS))
    logging.info("=" * 55)


    hilo = threading.Thread(target=iniciar_servidor_http, daemon=True)
    hilo.start()

    cargar_artistas_github()
    cargar_estado()

    # Comprobación al arrancar
    cambios_artistas = detectar_artistas_nuevos()
    comprobar_todos()

    # Comprobación automática diaria a las 17:50
    schedule.every().day.at("17:50").do(detectar_artistas_nuevos)
    schedule.every().day.at("17:50").do(comprobar_todos)

    logging.info("Scheduler activo. Comprobación automática a las 17:50 UTC.")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
