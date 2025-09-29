# src/scraper_mongo.py
"""
Refactor del scraper original: ahora inserta/actualiza directamente en MongoDB (bulk upsert)
- Mantiene la lógica de parseo robusta del scraper original.
- Por defecto guarda en Mongo (upsert). Puedes seguir guardando archivos locales con --save.
- Variables de entorno:
    MONGO_URI (default: mongodb://localhost:27017/)
    MONGO_DB  (default: loterias)
    MONGO_COLL_BASE (default: resultados_loterias)

Uso:
  python src/scraper_mongo.py --which 2        # extrae URL2 (bonoloto) y carga en Mongo
  python src/scraper_mongo.py --url "...+..." --save   # extrae varias urls, guarda archivos y carga en Mongo
  python src/scraper_mongo.py --which 1 --no-mongo      # extrae y solo guarda archivos (sin insertar en Mongo)

"""

import os
import re
import json
import argparse
from io import StringIO
from typing import List, Union, Dict, Any
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()
# Mongo
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError, PyMongoError

# ----------------- CONFIG: ajusta estas URLs a tus hojas -------------------
URL1 = os.environ.get("URL_SHEET_1",
    "https://docs.google.com/spreadsheets/u/0/d/1MVwwP3fsPK6Mcc3F0Fv1W6t92-PiTvfXjnZx0BAJOu0/pub?output=html&widget=true")
URL2 = os.environ.get("URL_SHEET_2",
    "https://docs.google.com/spreadsheets/u/0/d/175SqVQ3E7PFZ0ebwr2o98Kb6YEAwSUykGFh6ascEfI0/pub?output=html&widget=true")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"}
# -------------------------------------------------------------------------

# Mongo config
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "loterias")
MONGO_COLL_BASE = os.environ.get("MONGO_COLL_BASE", "resultados_loterias")

# local file paths (optional)
BASE = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE, "data", "raw")
PROC_DIR = os.path.join(BASE, "data", "processed")

_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

# -------------------- utilidades de fecha / parsing ----------------------
def _normalizar_fecha(fecha_str: str) -> Union[str, None]:
    if fecha_str is None:
        return None
    s = str(fecha_str).strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(",", " ").strip()

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    toks = s.lower().split()
    for i in range(len(toks)):
        if toks[i].isdigit() and i + 2 < len(toks):
            try:
                dia = int(toks[i])
                mes = _MESES.get(toks[i + 1])
                anio = int(re.sub(r'\D', '', toks[i + 2]))
                if mes:
                    return datetime(anio, mes, dia).strftime("%Y-%m-%d")
            except Exception:
                continue

    if re.fullmatch(r'^\d{4,5}$', s):
        try:
            serial = int(s)
            dt = datetime(1899, 12, 30) + timedelta(days=serial)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

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
    if text is None:
        return []
    toks = re.findall(r'\d+', str(text))
    return [int(t) for t in toks]

# -------------------- parseo de filas (cabecera esperada) ------------------
def _parse_row_from_cells(cells: List[str], juego_name: str) -> Union[dict, None]:
    if not cells or len(cells) < 2:
        return None
    cols = [("" if v is None else str(v)).strip() for v in cells]

    fecha = None
    fecha_idx = None
    for i in range(min(3, len(cols))):
        cand = cols[i]
        if re.fullmatch(r'^\d+(\.0+)?$', cand):
            continue
        norm = _normalizar_fecha(cand)
        if norm:
            fecha = norm
            fecha_idx = i
            break
    if fecha is None and cols:
        norm = _normalizar_fecha(cols[0])
        if norm:
            fecha = norm
            fecha_idx = 0
    if fecha is None:
        return None

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

    if len(numeros) < 6:
        candidate = []
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

    return {
        "juego": juego_name,
        "fecha": fecha,
        "numeros": [int(x) for x in numeros[:6]],
        "complementario": int(complementario) if complementario is not None else None,
        "reintegro": int(reintegro) if reintegro is not None else None
    }

# ------------------- obtener CSV desde puburl (varios gid) -----------------
def _try_csv_from_puburl(url: str) -> Union[str, None]:
    if not url:
        return None
    candidates = []
    if "pubhtml" in url:
        candidates.append(url.replace("pubhtml?output=html", "pub?output=csv"))
        candidates.append(url.replace("pubhtml?output=html", "pub?output=csv&single=true"))
    if "/pub" in url and "output=html" in url:
        candidates.append(url.replace("output=html", "output=csv"))

    m = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if m:
        sheet_id = m.group(1)
        candidates.append(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
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

    soup = BeautifulSoup(html_text, "html.parser")
    tables = soup.find_all("table")
    for table in tables:
        filas = table.find_all("tr")
        for tr in filas:
            cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            parsed = _parse_row_from_cells(cols, juego_name)
            if parsed:
                resultados.append(parsed)
    return resultados

# --------------------- extraer resultados de UNA URL (todas sus hojas) -----
def _extract_gids_from_html(html: str) -> List[str]:
    gids = set()
    if not html:
        return []
    for m in re.findall(r'gid=(\d+)', html):
        gids.add(m)
    for m in re.findall(r'export\?format=csv&gid=(\d+)', html):
        gids.add(m)
    for m in re.findall(r'"gid":\s*"?(\d+)"?', html):
        gids.add(m)
    return sorted(gids, key=int)


def obtener_todos_resultados_single(url: str, juego_name: str) -> List[dict]:
    if not url:
        return []

    combined_results: List[dict] = []
    try:
        resp_main = requests.get(url, headers=HEADERS, timeout=15)
        resp_main.raise_for_status()
        html_main = resp_main.text
    except Exception:
        html_main = None

    try:
        csv_text = _try_csv_from_puburl(url)
        if csv_text:
            res_csv = _parse_csv_text(csv_text, juego_name)
            if res_csv:
                combined_results.extend(res_csv)
    except Exception:
        pass

    if html_main:
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        sheet_id = m.group(1) if m else None

        gids = _extract_gids_from_html(html_main)
        if not gids and sheet_id:
            gids = [0,1]

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

    if html_main:
        try:
            import pandas as pd
            from io import StringIO
            dfs = pd.read_html(StringIO(html_main))
            for df in dfs:
                for _, row in df.iterrows():
                    cells = ["" if v is None else str(v) for v in row.tolist()]
                    parsed = _parse_row_from_cells(cells, juego_name)
                    if parsed:
                        combined_results.append(parsed)
        except Exception:
            pass

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

    if not combined_results:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
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
            pass

    if combined_results:
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
    out.sort(key=lambda r: r.get("fecha", ""), reverse=True)
    return out

# ----------------- Mongo helpers (similar to etl._make_doc_for_mongo) ------
def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_collection(prefix: str):
    name = f"{MONGO_COLL_BASE}_{prefix}"
    client = get_client()
    return client[MONGO_DB][name]


def _make_doc_for_mongo(row: Dict[str, Any]) -> Dict[str, Any]:
    nums = []
    if "numeros" in row and row.get("numeros"):
        nums = [int(x) for x in row.get("numeros")]
    else:
        for i in range(1, 7):
            k = f"n{i}"
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                try:
                    nums.append(int(v))
                except Exception:
                    pass

    fecha = row.get("fecha")
    fecha_norm = None
    if fecha:
        try:
            if isinstance(fecha, datetime):
                fecha_norm = fecha.strftime("%Y-%m-%d")
            else:
                import pandas as pd
                dt = pd.to_datetime(str(fecha), dayfirst=True, errors='coerce')
                if not pd.isna(dt):
                    fecha_norm = dt.strftime("%Y-%m-%d")
        except Exception:
            fecha_norm = None

    nums = [int(x) for x in nums][:6]
    clave_nums = ",".join(str(x) for x in nums) if nums else "no_nums"
    _id = f"{fecha_norm if fecha_norm else 'no_fecha'}:{clave_nums}"

    doc = {
        "_id": _id,
        "juego": row.get("juego", ""),
        "fecha": fecha_norm,
        "numeros": nums,
        "complementario": int(row.get("complementario")) if row.get("complementario") not in (None, "", "nan") else None,
        "reintegro": int(row.get("reintegro")) if row.get("reintegro") not in (None, "", "nan") else None,
        "fuente": row.get("fuente", ""),
        "inserted_at": datetime.now()
    }
    return doc

# --------------------- guardado raw + processed CSV (opcional) -----------
def _norm_and_save(resultados: List[dict], prefix: str = "primitiva"):
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROC_DIR, exist_ok=True)
    raw_path = os.path.join(RAW_DIR, f"{prefix}_raw.json")
    proc_path = os.path.join(PROC_DIR, f"{prefix}_processed.csv")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    print("Guardado raw JSON en:", raw_path)

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
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
        df = df[~df['fecha'].isna()].copy()
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df = df.sort_values('fecha').reset_index(drop=True)
        cols = ["juego", "fecha", "n1", "n2", "n3", "n4", "n5", "n6", "complementario", "reintegro"]
        df.to_csv(proc_path, index=False, columns=cols)
        print("Guardado processed CSV en:", proc_path)
    except Exception as e:
        print("No se pudo guardar CSV (pandas requerido):", e)

# ------------------ interfaz principal: varias URLs / alias ---------------
def obtener_todos_resultados(urls: Union[str, List[str]] = None, juego: str = None) -> List[dict]:
    if urls is None:
        urls = "1"
    if isinstance(urls, str) and "+" in urls:
        urls = [u.strip() for u in urls.split("+") if u.strip()]
    if isinstance(urls, str):
        urls = [urls]

    all_results = []
    for u in urls:
        actual_url = u
        if isinstance(u, str) and u.strip() in ("1", "2"):
            actual_url = URL1 if u.strip() == "1" else URL2
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

# ------------------ upsert to mongo -------------------------------------
def upsert_to_mongo(resultados: List[dict], prefix: str = "primitiva", ordered: bool = False) -> Dict[str, Any]:
    if not resultados:
        return {"ok": False, "reason": "no_results"}
    ops = []
    for r in resultados:
        try:
            doc = _make_doc_for_mongo(r)
        except Exception:
            continue
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    if not ops:
        return {"ok": False, "reason": "no_ops"}
    coll = get_collection(prefix)
    try:
        res = coll.bulk_write(ops, ordered=ordered)
        summary = {
            "ok": True,
            "n_ops": len(ops),
            "matched_count": getattr(res, "matched_count", None),
            "modified_count": getattr(res, "modified_count", None),
            "upserted_count": len(getattr(res, "upserted_ids", {}) or {})
        }
        return summary
    except BulkWriteError as bwe:
        return {"ok": False, "error": bwe.details}
    except PyMongoError as e:
        return {"ok": False, "error": str(e)}

# ------------------------------- CLI -------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper -> Mongo (upsert) — Primitiva/Bonoloto")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--which", choices=["1", "2"], help="Elegir URL1 (1) o URL2 (2)")
    group.add_argument("--url", help="URL completa (si quieres varias, sepáralas con '+')")
    parser.add_argument("--name", default=None, help="prefijo/nombre de salida (ej: primitiva_2013)")
    parser.add_argument("--no-mongo", action="store_true", help="no insertar en Mongo (solo guardar ficheros si --save)")
    parser.add_argument("--save", action="store_true", help="guardar raw JSON y processed CSV (en data/...)")
    parser.add_argument("--ordered", action="store_true", help="bulk_write ordered (más lento pero predecible).")
    args = parser.parse_args()

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

    if args.name:
        prefix = args.name
    else:
        if isinstance(urls_arg, str) and urls_arg.strip() == "2":
            prefix = "bonoloto"
        else:
            prefix = "primitiva"

    print("Leyendo:", urls_arg)
    todos = obtener_todos_resultados(urls_arg, juego=prefix)
    print("Filas obtenidas:", len(todos))
    if todos:
        print("Último:", todos[0])

    # por defecto insertamos en Mongo (salvo --no-mongo)
    if not args.no_mongo:
        print(f"Insertando {len(todos)} resultados en Mongo colección '{MONGO_COLL_BASE}_{prefix}' ...")
        res = upsert_to_mongo(todos, prefix=prefix, ordered=args.ordered)
        print("Resultado Mongo:", res)
    else:
        print("--no-mongo: no se insertará en Mongo.")

    if args.save:
        _norm_and_save(todos, prefix=prefix)
