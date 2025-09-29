# src/compara_resultados.py
import os
import json
import ast
import re
import pandas as pd
from datetime import datetime, timedelta

# ---------------- utilidades ----------------
def _parse_pred_field(raw_pred):
    """Normaliza diferentes representaciones de la predicción y devuelve lista de ints (máx 6)."""
    if raw_pred is None:
        return []
    if isinstance(raw_pred, list):
        return [int(x) for x in raw_pred][:6]
    s = str(raw_pred).strip()
    # intentar JSON
    try:
        val = json.loads(s)
        if isinstance(val, list):
            return [int(x) for x in val][:6]
    except Exception:
        pass
    # intentar literal_eval
    try:
        val = ast.literal_eval(s)
        if isinstance(val, list):
            return [int(x) for x in val][:6]
    except Exception:
        pass
    # fallback: extraer números por regexp
    nums = re.findall(r'\d+', s)
    return [int(x) for x in nums][:6]


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y normaliza nombres de columna (quita comillas/espacios/BOM)."""
    def _clean_colname(c):
        if not isinstance(c, str):
            return c
        c = c.lstrip('\ufeff').strip()
        c = c.strip().strip('"').strip("'").strip()
        return c
    df = df.copy()
    df.columns = [_clean_colname(c) for c in df.columns]
    return df


# ---------------- lógica principal ----------------
def compare_with_last(preds=None,
                      pred_file=None,
                      processed_prefix=None,
                      algorithm: str | None = None,
                      juego: str | None = None,
                      require_yesterday: bool = False):
    """
    Compara la predicción (específica por algoritmo y juego) generada AYER
    con el último resultado real.

    Args:
      preds: lista/string de predicción (si se quiere comparar algo en memoria).
      pred_file: ruta al CSV de predicciones (por defecto data/predicciones.csv).
      processed_prefix: prefijo para la función load_processed_df (si aplica).
      algorithm: 'sklearn' o 'keras' (filtra la fila de predicciones por algoritmo).
      juego: 'primitiva' o 'bonoloto' (filtra la fila de predicciones por juego).
      require_yesterday: si True falla si no hay predicción exactamente de ayer.

    Retorna dict con métricas.
    """
    # fechas: hoy/ayer en zona Madrid si está disponible
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("Europe/Madrid")).date()
    except Exception:
        today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # 1) obtener lista de predicción (pred_list)
    pred_list = None
    chosen_row = None

    if preds is not None:
        pred_list = _parse_pred_field(preds)
    else:
        # determinar ruta por defecto
        if pred_file is None:
            base = os.path.join(os.path.dirname(__file__), "..")
            pred_file = os.path.join(base, "data", "predicciones.csv")
        if not os.path.exists(pred_file):
            raise FileNotFoundError(f"No existe el fichero de predicciones: {pred_file}")

        # leer y normalizar cabeceras
        dfp = pd.read_csv(pred_file, dtype=str, skipinitialspace=True)
        dfp = _clean_columns(dfp)

        # intentar mapear nombres si están con otras variantes
        required_cols = {"prediccion", "fecha_predecida"}
        # heurística buscar 'algoritmo' y 'juego' si no existen exactamente
        if "algoritmo" not in dfp.columns:
            for c in dfp.columns:
                if "algori" in c.lower():
                    dfp = dfp.rename(columns={c: "algoritmo"})
                    break
        if "juego" not in dfp.columns:
            for c in dfp.columns:
                if c.lower().strip() in ("juego", "game"):
                    dfp = dfp.rename(columns={c: "juego"})
                    break

        if "fecha_predecida" not in dfp.columns or "prediccion" not in dfp.columns:
            raise ValueError(f"El fichero de predicciones no contiene las columnas esperadas. Cabeceras: {list(dfp.columns)}")

        # parsear fechas de predicción
        dfp["fecha_predecida_dt"] = pd.to_datetime(dfp["fecha_predecida"], errors="coerce").dt.date

        # filtrado por fecha = ayer
        mask_date = dfp["fecha_predecida_dt"] == yesterday
        candidates = dfp[mask_date].copy()

        # si hay filtro por algoritmo, aplicarlo (normalizado)
        if algorithm:
            algo_norm = algorithm.strip().lower()
            candidates = candidates[candidates.get("algoritmo", "").str.strip().str.lower() == algo_norm]

        # si hay filtro por juego, aplicarlo
        if juego:
            juego_norm = juego.strip().lower()
            candidates = candidates[candidates.get("juego", "").str.strip().str.lower() == juego_norm]

        # si no hay candidatos para ayer:
        if candidates.empty:
            if require_yesterday:
                raise ValueError(f"No se encontró predicción para {yesterday} con algoritmo='{algorithm}' juego='{juego}' en {pred_file}")
            # fallback: tomar la última predicción anterior a hoy que cumpla algoritmo+juego
            dfp_valid = dfp[dfp["fecha_predecida_dt"].notna() & (dfp["fecha_predecida_dt"] < today)].copy()
            if algorithm:
                dfp_valid = dfp_valid[dfp_valid.get("algoritmo", "").str.strip().str.lower() == algo_norm]
            if juego:
                dfp_valid = dfp_valid[dfp_valid.get("juego", "").str.strip().str.lower() == juego_norm]
            if dfp_valid.empty:
                raise ValueError(f"No se encontró ninguna predicción anterior a hoy que cumpla algoritmo='{algorithm}' y juego='{juego}'.")
            dfp_valid = dfp_valid.sort_values("fecha_predecida_dt")
            chosen_row = dfp_valid.iloc[-1].to_dict()
        else:
            # elegir la última de las de ayer (por si hay varias)
            candidates = candidates.sort_values("fecha_predecida_dt")
            chosen_row = candidates.iloc[-1].to_dict()

        raw_pred = chosen_row.get("prediccion", "")
        pred_list = _parse_pred_field(raw_pred)

    pred_list = [int(x) for x in pred_list][:6]

    # 2) cargar último resultado real (usar processed_prefix o juego si se pasó)
    from src.utils_ml import load_processed_df, df_to_numeros_list
    try:
        # preferir processed_prefix; si no, usar juego; si ninguno, dejar default
        prefix_for_load = processed_prefix or (juego if juego is not None else None)
        df_proc = load_processed_df(prefix_for_load) if prefix_for_load is not None else load_processed_df()
    except TypeError:
        df_proc = load_processed_df()

    nums_list = df_to_numeros_list(df_proc)
    if not nums_list:
        raise ValueError("No hay resultados reales en el CSV procesado.")
    last_real = nums_list[-1]

    # 3) métricas
    aciertos = set(pred_list) & set(last_real)
    precision = len(aciertos) / len(pred_list) if pred_list else 0.0
    recall = len(aciertos) / len(last_real) if last_real else 0.0

    out_lines = []
    out_lines.append("---- Comparación ----")
    if chosen_row:
        chosen_preview = {k: chosen_row.get(k) for k in ("prediccion", "fecha_predecida", "algoritmo", "juego")}
        out_lines.append(f"Predicción elegida (fila): {chosen_preview}")
    out_lines.append(f"Predicción (lista): {pred_list}")
    out_lines.append(f"Resultado real (último): {last_real}")
    out_lines.append(f"Aciertos: {sorted(aciertos)} ({len(aciertos)})")
    out_lines.append(f"Precisión={precision:.2f}, Recall={recall:.2f}")

    if len(aciertos) <= 2:
        out_lines.append("→ Sugerencia: ajustar hiperparámetros o probar otro WINDOW_K.")
    elif len(aciertos) >= 3:
        out_lines.append("→ Buen rendimiento, considerar mantener configuración.")

    # convertir a texto (con saltos de línea)
    out_text = "\n".join(out_lines)

    # imprimir por consola (igual que antes)
    print(out_text)
    try:
        base = os.path.join(os.path.dirname(__file__), "..")
        info_path = os.path.join(base, "data", "infocompare.txt")
        with open(info_path, "a", encoding="utf-8") as f:
            f.write(out_text + "\n")
        print(f"Guardado informe comparación en: {info_path}")
    except Exception as e:
        print("No se pudo guardar el informe de comparación:", e)
        
     
      # devolver también el texto en el dict de retorno
    return {
        "pred_row": chosen_row,
        "pred": pred_list,
        "last_real": last_real,
        "aciertos": sorted(aciertos),
        "precision": precision,
        "recall": recall,
        "info_text": out_text
    }


# ---------------- CLI ----------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Comparar predicción (ayer) con último resultado real.")
    parser.add_argument("--pred-file", help="Ruta al CSV de predicciones (por defecto data/predicciones.csv)")
    parser.add_argument("--prefix", help="Prefijo procesado (primitiva/bonoloto) si aplica")
    parser.add_argument("--algoritmo", help="Filtrar por algoritmo (sklearn / keras)")
    parser.add_argument("--juego", help="Filtrar por juego (primitiva / bonoloto)")
    parser.add_argument("--require-yesterday", action="store_true", help="Fallar si no hay predicción exactamente de ayer")
    args = parser.parse_args()
    compare_with_last(pred_file=args.pred_file,
                      processed_prefix=args.prefix,
                      algorithm=args.algoritmo,
                      juego=args.juego,
                      require_yesterday=args.require_yesterday)
