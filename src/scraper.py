# src/scraper.py
"""
Scraper robusto para Primitiva / Bonoloto (Google Sheets publicadas).
- Soporta 2 URLs maestras (URL1, URL2).
- Puede combinar varias hojas/tablas dentro de una misma URL.
- CLI interactiva: pregunta qué juego (1=primitiva, 2=bonoloto) si no se pasa por args.
- Guarda raw JSON y processed CSV en data/raw/ y data/processed/ con prefijo 'primitiva' o 'bonoloto'.
"""
import re
import os
import json
import argparse
from io import StringIO
from typing import List, Union
import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta

# ----------------- CONFIG: ajusta estas URLs a tus hojas -------------------
URL1 = os.environ.get("URL_SHEET_1",
    "https://docs.google.com/spreadsheets/u/0/d/1MVwwP3fsPK6Mcc3F0Fv1W6t92-PiTvfXjnZx0BAJOu0/pub?output=html&widget=true")
URL2 = os.environ.get("URL_SHEET_2",
    "https://docs.google.com/spreadsheets/u/0/d/175SqVQ3E7PFZ0ebwr2o98Kb6YEAwSUykGFh6ascEfI0/pub?output=html&widget=true")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"}
# -------------------------------------------------------------------------

# listado meses en español para parseo textual
_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

# -------------------- utilidades de fecha / parsing ----------------------
def _normalizar_fecha(fecha_str: str) -> Union[str, None]:
    """Intenta normalizar una string de fecha a 'YYYY-MM-DD'. Devuelve None si no es parseable."""
    if fecha_str is None:
        return None
    s = str(fecha_str).strip()
    if not s:
        return None

    # quitar sufijos/etiquetas comunes
    s = s.replace("\xa0", " ").replace(",", " ").strip()

    # formatos directos
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # formato textual español: "14 agosto 2013" o "jueves 14 agosto 2013"
    toks = s.lower().split()
    # buscar patrón dia mes año
    for i in range(len(toks)):
        if toks[i].isdigit() and i + 2 < len(toks):
            try:
                dia = int(toks[i])
                mes = _MESES.get(toks[i + 1])
                anio = int(re.sub(r'\D', '', toks[i + 2]))  # eliminar no dígitos
                if mes:
                    return datetime(anio, mes, dia).strftime("%Y-%m-%d")
            except Exception:
                continue

    # posible número serial Excel (por ejemplo 41275)
    if re.fullmatch(r'^\d{4,5}$', s):
        try:
            # Excel serial: fecha base 1899-12-30 (approx). Convertimos si razonable (>=1900)
            serial = int(s)
            dt = datetime(1899, 12, 30) + timedelta(days=serial)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # intento final: buscar explícitamente un patrón dd mm yyyy dentro del texto
    m = re.search(r'(\d{1,2})\D+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\D+(\d{4})', s, re.IGNORECASE)
    if m:
        try:
            dia = int(m.group(1))
            mes = _MESES.get(m.group(2).lower())
            anio = int(m.group(3))
            if mes:
                return datetime(anio, mes, dia).strftime("%Y-%m-%d")
        except Exception:
            pass

    return None

def _extract_ints_from_text(text: str) -> List[int]:
    """Extrae todos los enteros de una cadena en orden (ej: '3 15 23 26 34 38' -> [3,...])."""
    if text is None:
        return []
    toks = re.findall(r'\d+', str(text))
    return [int(t) for t in toks]

# -------------------- parseo de filas (cabecera esperada) ------------------
def _parse_row_from_cells(cells: List[str], juego_name: str) -> Union[dict, None]:
    """
    Heurística para filas con cabecera: FECHA, COMB. GANADORA, COMP., R.
    - cells: lista de strings (texto de celdas en la fila)
    - devuelve dict o None si no es una fila válida
    """
    if not cells or len(cells) < 2:
        return None

    # normalizar celdas (strip)
    cols = [("" if v is None else str(v)).strip() for v in cells]

    # 1) encontrar fecha en las primeras 3 columnas (evitar índices como '999.0')
    fecha = None
    fecha_idx = None
    for i in range(min(3, len(cols))):
        cand = cols[i]
        # descartar candidatos que sean solo números tipo '999.0' o '1' (posibles índices)
        if re.fullmatch(r'^\d+(\.0+)?$', cand):
            continue
        norm = _normalizar_fecha(cand)
        if norm:
            fecha = norm
            fecha_idx = i
            break
    # si no hemos encontrado fecha, intentar parsear la primera aunque sea numérica (fallback)
    if fecha is None and cols:
        norm = _normalizar_fecha(cols[0])
        if norm:
            fecha = norm
            fecha_idx = 0

    if fecha is None:
        # no hay fecha válida -> ignorar fila
        return None

    # 2) extraer números: preferimos la celda inmediatamente posterior a la fecha
    numeros = []
    complementario = None
    reintegro = None

    if fecha_idx + 1 < len(cols):
        nums_comb = _extract_ints_from_text(cols[fecha_idx + 1])
        if len(nums_comb) >= 6:
            numeros = nums_comb[:6]
            if len(nums_comb) >= 7:
                complementario = nums_comb[6]
            if len(nums_comb) >= 8:
                reintegro = nums_comb[7]

    # 3) si no encontramos 6 nums, intentar recoger en las celdas siguientes individualmente
    if len(numeros) < 6:
        candidate = []
        # mirar hasta 10 celdas después de fecha
        for c in cols[fecha_idx + 1: fecha_idx + 1 + 10]:
            candidate += _extract_ints_from_text(c)
            if len(candidate) >= 8:
                break
        if len(candidate) >= 6:
            numeros = candidate[:6]
            if len(candidate) >= 7:
                complementario = candidate[6]
            if len(candidate) >= 8:
                reintegro = candidate[7]

    # 4) última comprobación intentando posiciones fijas (por si el layout fuera: fecha,n1,n2,n3,n4,n5,n6,comp,reint)
    if len(numeros) < 6 and len(cols) >= 9:
        cand = []
        for c in cols[1:8]:
            cand += _extract_ints_from_text(c)
        if len(cand) >= 6:
            numeros = cand[:6]
            if len(cand) >= 7:
                complementario = cand[6]
            if len(cand) >= 8:
                reintegro = cand[7]

    if not numeros or len(numeros) < 6:
        return None

    # normalizar tipos y return
    return {
        "juego": juego_name,
        "fecha": fecha,
        "numeros": [int(x) for x in numeros[:6]],
        "complementario": int(complementario) if complementario is not None else None,
        "reintegro": int(reintegro) if reintegro is not None else None
    }

# ------------------- obtener CSV desde puburl (varios gid) -----------------
def _try_csv_from_puburl(url: str) -> Union[str, None]:
    """
    Intenta varias variantes de la URL pública para obtener CSV.
    Prueba export?format=csv y gid=0..N.
    """
    if not url:
        return None
    candidates = []

    # variantes directas
    if "pubhtml" in url:
        candidates.append(url.replace("pubhtml?output=html", "pub?output=csv"))
        candidates.append(url.replace("pubhtml?output=html", "pub?output=csv&single=true"))
    if "/pub" in url and "output=html" in url:
        candidates.append(url.replace("output=html", "output=csv"))

    # intentar export endpoint con varios gid
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if m:
        sheet_id = m.group(1)
        candidates.append(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
        # probar varios gids (por si hay varias hojas)
        for gid in range(0, 8):
            candidates.append(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}")

    for c in candidates:
        try:
            resp = requests.get(c, headers=HEADERS, timeout=15)
            if resp.status_code == 200 and ("," in resp.text or "\n" in resp.text):
                return resp.text
        except Exception:
            continue
    return None

# ----------------- parsear CSV (texto) a resultados -----------------------
def _parse_csv_text(csv_text: str, juego_name: str) -> List[dict]:
    """
    Lee CSV con pandas y parsea fila a fila usando _parse_row_from_cells.
    """
    try:
        import pandas as pd
        df = pd.read_csv(StringIO(csv_text), sep=None, engine="python", encoding="utf-8")
    except Exception:
        try:
            import pandas as pd
            df = pd.read_csv(StringIO(csv_text), encoding="latin-1")
        except Exception:
            return []

    resultados = []
    for _, row in df.iterrows():
        cells = ["" if v is None else str(v) for v in row.tolist()]
        parsed = _parse_row_from_cells(cells, juego_name)
        if parsed:
            resultados.append(parsed)
    return resultados

# ----------------- parsear todas las tablas HTML (pandas + BS) -------------
def _parse_html_tables_all(html_text: str, juego_name: str) -> List[dict]:
    resultados = []
    # intentar pandas.read_html para todas las tablas
    try:
        import pandas as pd
        dfs = pd.read_html(StringIO(html_text))
        for df in dfs:
            for _, row in df.iterrows():
                cells = ["" if v is None else str(v) for v in row.tolist()]
                parsed = _parse_row_from_cells(cells, juego_name)
                if parsed:
                    resultados.append(parsed)
        if resultados:
            return resultados
    except Exception:
        pass

    # fallback: BeautifulSoup tomar todas las tablas <table>
    soup = BeautifulSoup(html_text, "html.parser")
    tables = soup.find_all("table")
    for table in tables:
        filas = table.find_all("tr")
        # ignorar cabeceras: procesamos todas y el parser decidirá
        for tr in filas:
            cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            parsed = _parse_row_from_cells(cols, juego_name)
            if parsed:
                resultados.append(parsed)
    return resultados

# --------------------- extraer resultados de UNA URL (todas sus hojas) -----
# Añadir esta función auxiliar cerca de las utilidades existentes
def _extract_gids_from_html(html: str) -> List[str]:
    """
    Extrae todos los gid que aparecen en una página pubhtml (hrefs, iframes, scripts).
    Devuelve lista de gid (strings) únicos.
    """
    gids = set()
    if not html:
        return []
    # buscar patrones gid=12345
    for m in re.findall(r'gid=(\d+)', html):
        gids.add(m)
    # buscar patrones /pub?gid=12345 o export?gid=12345
    for m in re.findall(r'export\?format=csv&gid=(\d+)', html):
        gids.add(m)
    # algunas páginas incluyen data-sheet-id/data-gid en JS
    for m in re.findall(r'"gid":\s*"?(\d+)"?', html):
        gids.add(m)
    # devolver como lista ordenada
    return sorted(gids, key=int)


# Reemplaza la implementación antigua por esta función
def obtener_todos_resultados_single(url: str, juego_name: str) -> List[dict]:
    """
    Extrae resultados de una sola URL (puede contener varias hojas).
    - Intenta export CSV sin gid, luego intenta todos los gid encontrados en la página,
      luego intenta pandas.read_html para todas las tablas y finalmente BS.
    - Devuelve lista combinada (ordenada por fecha desc) o [].
    """
    if not url:
        return []

    combined_results: List[dict] = []

    # 0) obtener HTML una vez (para extraer gids y para read_html)
    try:
        resp_main = requests.get(url, headers=HEADERS, timeout=15)
        resp_main.raise_for_status()
        html_main = resp_main.text
    except Exception:
        html_main = None

    # 1) intentar CSV "global" (sin gid) - a veces contiene toda la hoja por defecto
    try:
        csv_text = _try_csv_from_puburl(url)
        if csv_text:
            res_csv = _parse_csv_text(csv_text, juego_name)
            if res_csv:
                combined_results.extend(res_csv)
    except Exception:
        pass

    # 2) si tenemos HTML principal, extraer todos los gid y tratar export csv por cada gid
    if html_main:
        # sacar sheet_id si es posible
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        sheet_id = m.group(1) if m else None

        gids = _extract_gids_from_html(html_main)
        # si no encontramos gids vía búsqueda, pero tenemos sheet_id, probamos algunos gid comunes
        if not gids and sheet_id:
            # probamos varios gids amplios (0..20) si no hay indicios
            gids = [0,1]

        # intentar export CSV para cada gid (si hay sheet_id)
        if sheet_id:
            tried = set()
            for gid in gids:
                if gid in tried:
                    continue
                tried.add(gid)
                export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
                try:
                    resp = requests.get(export_url, headers=HEADERS, timeout=12)
                    if resp.status_code == 200 and ("," in resp.text or "\n" in resp.text):
                        res_gid = _parse_csv_text(resp.text, juego_name)
                        if res_gid:
                            combined_results.extend(res_gid)
                except Exception:
                    continue

    # 3) intentar pandas.read_html sobre el HTML principal y combinar todas las tablas encontradas
    if html_main:
        try:
            import pandas as pd
            from io import StringIO
            dfs = pd.read_html(StringIO(html_main))
            for df in dfs:
                # parsear todas las filas del DataFrame
                for _, row in df.iterrows():
                    cells = ["" if v is None else str(v) for v in row.tolist()]
                    parsed = _parse_row_from_cells(cells, juego_name)
                    if parsed:
                        combined_results.append(parsed)
        except Exception:
            pass

    # 4) fallback BeautifulSoup: recorrer todas las tablas <table> en el HTML
    if html_main:
        try:
            soup = BeautifulSoup(html_main, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                filas = table.find_all("tr")
                for tr in filas:
                    cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
                    parsed = _parse_row_from_cells(cols, juego_name)
                    if parsed:
                        combined_results.append(parsed)
        except Exception:
            pass

    # 5) si aún vacío y no teníamos html_main (requests falló antes), intentar hacer un GET simple y parsear HTML
    if not combined_results:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
            # intentar pandas.read_html o BS como último recurso
            try:
                import pandas as pd
                from io import StringIO
                dfs = pd.read_html(StringIO(html))
                for df in dfs:
                    for _, row in df.iterrows():
                        cells = ["" if v is None else str(v) for v in row.tolist()]
                        parsed = _parse_row_from_cells(cells, juego_name)
                        if parsed:
                            combined_results.append(parsed)
            except Exception:
                soup = BeautifulSoup(html, "html.parser")
                tables = soup.find_all("table")
                for table in tables:
                    filas = table.find_all("tr")
                    for tr in filas:
                        cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
                        parsed = _parse_row_from_cells(cols, juego_name)
                        if parsed:
                            combined_results.append(parsed)
        except Exception:
            # no hacer raise; devolver lista vacía para que el llamador muestre advertencia
            pass

    # ordenar y deduplicar localmente (por si combinamos varias fuentes duplicadas)
    if combined_results:
        # quitar duplicados simples aquí (fecha + numeros)
        unique = {}
        for r in combined_results:
            f = r.get("fecha")
            nums = tuple(r.get("numeros", []))
            if not f or not nums:
                continue
            key = (f, nums)
            if key not in unique:
                unique[key] = r
        out = list(unique.values())
        out.sort(key=lambda r: r.get("fecha", ""), reverse=True)
        return out

    return []


# -------------------- deduplicación y combinación --------------------------
def _deduplicate_resultados(resultados: List[dict]) -> List[dict]:
    seen = {}
    out = []
    for r in resultados:
        fecha = r.get("fecha")
        nums = r.get("numeros", [])
        if not fecha or not nums:
            continue
        key = (fecha, tuple(nums))
        if key not in seen:
            seen[key] = r
            out.append(r)
    # ordenar por fecha desc
    out.sort(key=lambda r: r.get("fecha", ""), reverse=True)
    return out

# ------------------ interfaz principal: varias URLs / alias ---------------
def obtener_todos_resultados(urls: Union[str, List[str]] = None, juego: str = None) -> List[dict]:
    """
    urls: None (usa URL1 por defecto), or "1"/"2" (alias), or "urlA+urlB" string, or list of urls.
    juego: optional override name ('primitiva'/'bonoloto'); if None, infer from which alias used.
    """
    if urls is None:
        urls = "1"  # por defecto URL1

    # permitir "1+2" en una sola string
    if isinstance(urls, str) and "+" in urls:
        urls = [u.strip() for u in urls.split("+") if u.strip()]

    if isinstance(urls, str):
        urls = [urls]

    all_results = []
    for u in urls:
        # alias '1' '2'
        actual_url = u
        if isinstance(u, str) and u.strip() in ("1", "2"):
            actual_url = URL1 if u.strip() == "1" else URL2

        # inferir nombre de juego si no se pasó explicitamente
        juego_name = juego
        if juego_name is None:
            juego_name = "primitiva" if str(u).strip() == "1" else "bonoloto" if str(u).strip() == "2" else "primitiva"

        try:
            res = obtener_todos_resultados_single(actual_url, juego_name)
            if res:
                all_results.extend(res)
        except Exception as e:
            print(f"[warning] fallo extrayendo {actual_url}: {e}")

    combined = _deduplicate_resultados(all_results)
    return combined

def obtener_ultimo_resultado(urls: Union[str, List[str]] = None, juego: str = None) -> Union[dict, None]:
    res = obtener_todos_resultados(urls=urls, juego=juego)
    return res[0] if res else None

# --------------------- guardado raw + processed CSV -----------------------
def _norm_and_save(resultados: List[dict], prefix: str = "primitiva"):
    """
    Guarda JSON raw y CSV procesado en:
      data/raw/{prefix}_raw.json
      data/processed/{prefix}_processed.csv
    CSV columnas: juego,fecha,n1..n6,complementario,reintegro
    """
    base = os.path.join(os.path.dirname(__file__), "..")
    raw_dir = os.path.join(base, "data", "raw")
    proc_dir = os.path.join(base, "data", "processed")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(proc_dir, exist_ok=True)

    raw_path = os.path.join(raw_dir, f"{prefix}_raw.json")
    proc_path = os.path.join(proc_dir, f"{prefix}_processed.csv")

    # guardar JSON raw (listado)
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    print("Guardado raw JSON en:", raw_path)

    # preparar filas para CSV
    rows = []
    for r in resultados:
        fecha = r.get("fecha")
        if not fecha:
            continue
        nums = r.get("numeros", [])
        if not nums or len(nums) < 6:
            continue
        rows.append({
            "juego": r.get("juego", prefix),
            "fecha": fecha,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
            "complementario": r.get("complementario"),
            "reintegro": r.get("reintegro")
        })

    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        if df.empty:
            print("No hay filas válidas para guardar en CSV.")
            return
        # asegurar formato fecha y ordenar asc
        """df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
        df = df[~df['fecha'].isna()].copy()
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df = df.sort_values('fecha').reset_index(drop=True)"""
                # convertir a datetime (naive por defecto)
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
        # eliminar filas sin fecha válida
        df = df[~df['fecha'].isna()].copy()

        # convertir a date objects (sin tz) y filtrar por fecha <= hoy en Europa/Madrid
        try:
            # obtener la fecha 'hoy' en zona Europe/Madrid (date)
            from zoneinfo import ZoneInfo  # Py3.9+
            today_date = datetime.now(ZoneInfo("Europe/Madrid")).date()
        except Exception:
            today_date = datetime.now().date()

        # crear columna auxiliar de tipo date (sin zona) y filtrar
        df['fecha_date'] = df['fecha'].dt.date
        # conservar solo fechas <= today_date (descartar filas a futuro)
        df = df[df['fecha_date'] <= today_date].copy()

        # formatear fecha y ordenar ascendente
        df['fecha'] = df['fecha_date'].apply(lambda d: d.strftime('%Y-%m-%d'))
        df = df.sort_values('fecha').reset_index(drop=True)
        # eliminar columna auxiliar
        df.drop(columns=['fecha_date'], inplace=True)


        
        cols = ["juego", "fecha", "n1", "n2", "n3", "n4", "n5", "n6", "complementario", "reintegro"]
        df.to_csv(proc_path, index=False, columns=cols)
        print("Guardado processed CSV en:", proc_path)
    except Exception as e:
        print("No se pudo guardar CSV (pandas requerido):", e)

# ------------------------------- CLI -------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper Primitiva/Bonoloto — elegir juego y guardar")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--which", choices=["1", "2"], help="Elegir URL1 (1) o URL2 (2)")
    group.add_argument("--url", help="URL completa (si quieres varias, sepáralas con '+')")
    parser.add_argument("--name", default=None, help="prefijo/nombre de fichero de salida (ej: primitiva_2013)")
    parser.add_argument("--save", action="store_true", help="guardar raw JSON y processed CSV (en data/...)")
    args = parser.parse_args()

    # si no se pasa --which / --url, pedir interactivamente qué juego extraer
    if not args.which and not args.url:
        print("Selecciona el juego a extraer:")
        print("  1) primitiva")
        print("  2) bonoloto")
        choice = input("Elige (1/2) [1]: ").strip() or "1"
        if choice not in ("1", "2"):
            print("Opción inválida, se toma '1' (primitiva).")
            choice = "1"
        urls_arg = choice
    else:
        urls_arg = args.which if args.which else args.url

    # inferir nombre de juego y prefijo
    if args.name:
        prefix = args.name
    else:
        # si urls_arg es '1' o '2' usa nombre amigable
        if isinstance(urls_arg, str) and urls_arg.strip() == "2":
            prefix = "bonoloto"
        else:
            prefix = "primitiva"

    print("Leyendo:", urls_arg)
    todos = obtener_todos_resultados(urls_arg, juego=prefix)
    print("Filas obtenidas:", len(todos))
    if todos:
        print("Último:", todos[0])

    if args.save:
        _norm_and_save(todos, prefix=prefix)
