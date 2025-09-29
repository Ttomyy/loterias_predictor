import os
import numpy as np
import pandas as pd

BASE = os.path.join(os.path.dirname(__file__), '..')
def _processed_csv_for(prefix: str | None = None) -> str:
    """
    Devuelve la ruta al CSV procesado basado en 'prefix'.
    Si prefix es None, se usa la variable de entorno JUEGO o 'primitiva' por defecto.
    """
    p = prefix or os.environ.get("JUEGO") or "primitiva"
    return os.path.join(BASE, "data", "processed", f"{p}_processed.csv")

#PROCESSED_CSV = os.path.join(BASE, 'data', 'processed', 'primitiva_processed.csv')
NUM_MAX = 49
""""
def load_processed_df():
    df = pd.read_csv(PROCESSED_CSV, parse_dates=['fecha'], dayfirst=True)
    df = df.sort_values('fecha').reset_index(drop=True)
    return df  """""


def load_processed_df(prefix: str | None = None) -> pd.DataFrame:
    """
    Carga el CSV procesado correspondiente al 'prefix' (o a la var de entorno JUEGO si no se pasa).
    Devuelve DataFrame con columna 'fecha' parseada y ordenado ascendantemente por fecha.
    """
    path = _processed_csv_for(prefix)
    if not os.path.exists(path):
        # mensaje útil para debugging
        files = []
        proc_dir = os.path.join(BASE, "data", "processed")
        if os.path.isdir(proc_dir):
            files = os.listdir(proc_dir)
        raise FileNotFoundError(
            f"No se encontró el CSV procesado en '{path}'. "
            f"Archivos disponibles en data/processed: {files}"
        )

    df = pd.read_csv(path, parse_dates=["fecha"], dayfirst=True)
    df = df.sort_values("fecha").reset_index(drop=True)
    return df

#------------ funciones auxiliares para features.py y train_sklearn.py
def df_to_numeros_list(df):
    numeros_list = []
    for _, row in df.iterrows():
        nums = []
        for c in ['n1','n2','n3','n4','n5','n6']:
            v = row.get(c)
            if pd.isna(v):
                continue
            nums.append(int(v))
        numeros_list.append(nums)
    return numeros_list

def make_onehot_draw(draw):
    """Dado draw (lista de números), devuelve vector one-hot length NUM_MAX."""
    v = np.zeros(NUM_MAX, dtype=np.int8)
    for n in draw:
        v[n-1] = 1
    return v