import os
import joblib
import numpy as np
#from utils_ml import load_processed_df, df_to_numeros_list
try:
    # cuando se ejecuta como paquete (python -m src.predict_keras)
    from src.utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw
except Exception:
    # cuando se ejecuta directamente (python src/predict_keras.py)
    from utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw

try:
    from src.compara_resultados import compare_with_last
except Exception:
    # si ejecutas el script directamente (no como paquete), usar import relativo
    from compara_resultados import compare_with_last
        
import json
import csv
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None
    
MODEL_FILE = os.path.join(os.path.dirname(__file__), '..', 'models', 'rf_multijoblib.pkl')
WINDOW_K = 8
NUM_MAX = 49
PRED_FILE = os.path.join(os.path.dirname(__file__), '..', 'data','predicciones.csv')  


def _today_madrid_iso():
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Europe/Madrid")).date().isoformat()
    except Exception:
        pass
    return datetime.now().date().isoformat()

def append_prediction(prediction, fecha_predecida, algoritmo, juego=None, path=PRED_FILE):
    """
    Añade una fila (append) sin cabecera al fichero `path`.
    Nuevo formato: "prediccion","fecha_predecida","algoritmo","juego"
    prediccion se escribe como JSON: "[1,2,3,4,5,6]"
    """
    if juego is None:
        juego = os.environ.get("JUEGO", "primitiva")  # por defecto primitiva
    row = [json.dumps(prediction, ensure_ascii=False), fecha_predecida, algoritmo, juego]
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(row)

def build_last_feature():
    df = load_processed_df()
    nums = df_to_numeros_list(df)
    prev = nums[-WINDOW_K:]
    counts = np.zeros(NUM_MAX, dtype=int)
    for draw in prev:
        for n in draw:
            counts[n-1] += 1
    last = np.zeros(NUM_MAX, dtype=int)
    for n in prev[-1]:
        last[n-1] = 1
    idx_norm = np.array([len(nums) / max(1, len(nums))])
    feat = np.concatenate([counts, last, idx_norm])
    return feat.reshape(1, -1)


def predict_next(top_k=6):
    if not os.path.exists(MODEL_FILE):
        raise FileNotFoundError('Entrena el modelo sklearn primero (train_sklearn.py)')
    clf = joblib.load(MODEL_FILE)
    X = build_last_feature()
    try:
        prob_list = [est.predict_proba(X)[:,1] for est in clf.estimators_]
        probs = np.array(prob_list).flatten()
    except Exception:
        preds = clf.predict(X).flatten()
        if preds.sum() >= top_k:
            chosen = np.where(preds == 1)[0] + 1
            return chosen[:top_k].tolist()
        # fallback
        probs = np.random.rand(NUM_MAX)
    idx = np.argsort(probs)[::-1][:top_k] + 1
    return idx.tolist()

'''
def compare_with_last(preds):
    df = load_processed_df()
    last_real = df_to_numeros_list(df)[-1]
    aciertos = set(preds) & set(last_real)
    print(last_real)
    precision = len(aciertos) / len(preds)
    recall = len(aciertos) / len(last_real)
    print("Predicción:", preds)
    print("Resultado real:", last_real)
    print("Aciertos:", aciertos, f"({len(aciertos)})")
    print(f"Precisión={precision:.2f}, Recall={recall:.2f}")
    if len(aciertos) <= 2:
        print("→ Sugerencia: aumentar n_estimators o ajustar WINDOW_K.")
    elif len(aciertos) >= 4:
        print("→ Buen rendimiento, mantener o probar con más datos históricos.")
'''
if __name__ == '__main__':
    print('SKLearn sugerencia:', predict_next())
    preds = predict_next()
    juego_env = os.environ.get("JUEGO", "primitiva")
    compare_with_last(algorithm="sklearn",juego=juego_env )
    fecha = _today_madrid_iso()
    append_prediction(preds, fecha, "sklearn", juego=juego_env)  # o "keras" en el otro fichero

