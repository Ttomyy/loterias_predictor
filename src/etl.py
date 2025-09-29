# src/etl.py
"""
ETL bidireccional:
 - mongo2fs (por defecto): lee colección Mongo -> guarda data/raw/{prefix}_raw.json y data/processed/{prefix}_processed.csv
 - fs2mongo (--to-mongo): lee CSV/JSON desde data/processed o data/raw -> inserta/bulk_upsert en Mongo
Usa --which/--game para seleccionar 'primitiva' o 'bonoloto' (o interactivo).
"""
import os
import json
import argparse
from datetime import datetime
from typing import List, Dict, Any
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError, PyMongoError
from dotenv import load_dotenv
load_dotenv()
# CONFIG vía env
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "loterias")
COLLECTION_BASE = os.environ.get("MONGO_COLL_BASE", "resultados_loterias")

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
OUT_DIR_RAW = os.path.join(BASE_DIR, "data", "raw")
OUT_DIR_PROCESSED = os.path.join(BASE_DIR, "data", "processed")


# ---------- utilidades Mongo ----------
def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def get_collection(prefix: str):
    name = f"{COLLECTION_BASE}_{prefix}"
    client = get_client()
    return client[MONGO_DB][name]


def _make_doc_for_mongo(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza fila/registro a documento de Mongo listo para insertar.
    Se espera keys: juego, fecha (YYYY-MM-DD o parseable), n1..n6 OR 'numeros' list, complementario, reintegro, fuente
    Genera _id = "YYYY-MM-DD:3,15,23,26,34,38"
    """
    # manejar si vienen n1..n6
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

    # fecha: intentar normalizar a YYYY-MM-DD
    fecha = row.get("fecha")
    fecha_norm = None
    if fecha:
        try:
            # si ya es datetime
            if isinstance(fecha, datetime):
                fecha_norm = fecha.strftime("%Y-%m-%d")
            else:
                import pandas as pd
                dt = pd.to_datetime(str(fecha), dayfirst=True, errors='coerce')
                if not pd.isna(dt):
                    fecha_norm = dt.strftime("%Y-%m-%d")
        except Exception:
            fecha_norm = None

    # asegurar lista de 6 ints (o menos si no hay)
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


# ---------- mongo -> files (original) ----------
def fetch_all_from_mongo(prefix: str) -> List[Dict[str, Any]]:
    col = get_collection(prefix)
    docs = list(col.find({}, {"_id": 0}))
    return docs

def mongo_to_files(prefix: str) -> str:
    """
    Lee toda la colección y guarda raw JSON y processed CSV (equivalente a tu código original).
    Returns path to processed CSV or None.
    """
    os.makedirs(OUT_DIR_RAW, exist_ok=True)
    os.makedirs(OUT_DIR_PROCESSED, exist_ok=True)

    docs = fetch_all_from_mongo(prefix)
    if not docs:
        print(f"No se han encontrado documentos en MongoDB para {prefix}.")
        return None

    raw_path = os.path.join(OUT_DIR_RAW, f"{prefix}_raw.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2, default=str)
    print("Guardado raw en:", raw_path)

    # Normalizar y crear filas tipo n1..n6
    rows = []
    for r in docs:
        nums = r.get("numeros") or []
        if isinstance(nums, str):
            nums = [int(x) for x in nums.split() if x.isdigit()]
        nums = [int(x) for x in nums][:6]
        rows.append({
            "juego": r.get("juego", ""),
            "fecha": r.get("fecha"),
            "n1": nums[0] if len(nums) > 0 else None,
            "n2": nums[1] if len(nums) > 1 else None,
            "n3": nums[2] if len(nums) > 2 else None,
            "n4": nums[3] if len(nums) > 3 else None,
            "n5": nums[4] if len(nums) > 4 else None,
            "n6": nums[5] if len(nums) > 5 else None,
            "complementario": r.get("complementario"),
            "reintegro": r.get("reintegro"),
            "fuente": r.get("fuente", "")
        })
    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
        df = df[~df['fecha'].isna()].copy()
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        df = df.sort_values('fecha').reset_index(drop=True)
        processed_path = os.path.join(OUT_DIR_PROCESSED, f"{prefix}_processed.csv")
        df.to_csv(processed_path, index=False)
        print("Guardado processed en:", processed_path)
        return processed_path
    except Exception as e:
        print("Error guardando CSV (pandas requerido):", e)
        return None


# ---------- files -> mongo (nuevo) ----------
def _load_from_files(prefix: str) -> List[Dict[str, Any]]:
    """
    Intenta cargar primero processed CSV, si no existe intenta raw JSON.
    Devuelve lista de dicts uniformes.
    """
    csv_path = os.path.join(OUT_DIR_PROCESSED, f"{prefix}_processed.csv")
    raw_path = os.path.join(OUT_DIR_RAW, f"{prefix}_raw.json")
    rows = []
    if os.path.exists(csv_path):
        try:
            import pandas as pd
            df = pd.read_csv(csv_path, dtype=str)
            for _, r in df.iterrows():
                # mapear columnas a formato de documento
                nums = []
                for i in range(1,7):
                    k = f"n{i}"
                    v = r.get(k)
                    if v is not None and str(v).strip() != "":
                        try:
                            nums.append(int(float(v)))
                        except Exception:
                            pass
                rows.append({
                    "juego": r.get("juego", prefix),
                    "fecha": r.get("fecha"),
                    "numeros": nums,
                    "complementario": int(r.get("complementario")) if r.get("complementario") not in (None, "", "nan") else None,
                    "reintegro": int(r.get("reintegro")) if r.get("reintegro") not in (None, "", "nan") else None,
                    "fuente": r.get("fuente", "")
                })
            if rows:
                return rows
        except Exception as e:
            print("Error leyendo CSV:", e)

    # fallback: intentar raw json
    if os.path.exists(raw_path):
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for r in data:
                rows.append({
                    "juego": r.get("juego", prefix),
                    "fecha": r.get("fecha"),
                    "numeros": r.get("numeros") or [],
                    "complementario": r.get("complementario"),
                    "reintegro": r.get("reintegro"),
                    "fuente": r.get("fuente", "")
                })
            return rows
        except Exception as e:
            print("Error leyendo raw JSON:", e)

    print("No se encontraron ficheros para", prefix)
    return []


def files_to_mongo(prefix: str, ordered: bool = False) -> Dict[str, Any]:
    """
    Carga filas desde data y las inserta/actualiza en Mongo (bulk upsert).
    Devuelve resumen.
    """
    rows = _load_from_files(prefix)
    if not rows:
        return {"ok": False, "reason": "no_files_or_no_rows"}

    # construir ops ReplaceOne con _id único
    ops = []
    for r in rows:
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


# ------------------ CLI ------------------
def _interactive_choice() -> str:
    print("Selecciona el juego:")
    print(" 1) primitiva")
    print(" 2) bonoloto")
    ch = input("Elige (1/2) [1]: ").strip() or "1"
    if ch not in ("1","2"):
        ch = "1"
    return "primitiva" if ch == "1" else "bonoloto"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL bidireccional: mongo<->files")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--which", choices=["1","2"], help="1=primitiva 2=bonoloto")
    group.add_argument("--game", choices=["primitiva","bonoloto"], help="nombre juego")
    parser.add_argument("--prefix", default=None, help="prefijo para ficheros y colección")
    parser.add_argument("--to-mongo", action="store_true", help="cargar desde data/ -> Mongo (fs2mongo).")
    parser.add_argument("--ordered", action="store_true", help="bulk_write ordered (más lento pero predecible).")
    args = parser.parse_args()

    if args.which:
        prefix = "primitiva" if args.which == "1" else "bonoloto"
    elif args.game:
        prefix = args.game
    else:
        prefix = _interactive_choice()

    if args.prefix:
        out_prefix = args.prefix
    else:
        out_prefix = prefix

    if args.to_mongo:
        print(f"Importando archivos '{out_prefix}' -> Mongo colección '{COLLECTION_BASE}_{prefix}' ...")
        r = files_to_mongo(out_prefix, ordered=args.ordered)
        print("Resultado:", r)
    else:
        print(f"Exportando Mongo '{COLLECTION_BASE}_{prefix}' -> data/raw + data/processed (prefijo {out_prefix}) ...")
        p = mongo_to_files(prefix)
        if p and out_prefix != prefix:
            # renombrar si usuario pidió otro prefijo
            try:
                dst = os.path.join(OUT_DIR_PROCESSED, f"{out_prefix}_processed.csv")
                os.replace(p, dst)
                print(f"Renombrado processed a: {dst}")
            except Exception:
                pass
