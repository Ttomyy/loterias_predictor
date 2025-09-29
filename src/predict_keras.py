# src/predict_keras.py
import os, numpy as np
from tensorflow import keras
#from utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw
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

MODEL_KERAS_FILE = os.path.join(os.path.dirname(__file__), '..', 'models', 'keras_lstm.keras')
MODEL_TF_DIR = os.path.join(os.path.dirname(__file__), '..', 'models', 'keras_lstm_tf')
MODEL_H5 = os.path.join(os.path.dirname(__file__), '..', 'models', 'keras_lstm.h5')
WINDOW_K = 8
NUM_MAX = 49
PRED_FILE = os.path.join(os.path.dirname(__file__), '..', 'data','predicciones.csv')  # sin extensión según lo solicitado


def _today_madrid_iso():
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Europe/Madrid")).date().isoformat()
    except Exception:
        pass
    return datetime.now().date().isoformat()

def append_prediction(prediction, fecha_predecida, algoritmo,juego=None, path=PRED_FILE):
    """
    Añade una fila (append) sin cabecera al fichero `path`.
    Formato: "prediccion","fecha_predecida","algoritmo"
    prediccion se escribe como JSON: "[1,2,3,4,5,6]"
    """
    if juego is None:
        juego = os.environ.get("JUEGO", "primitiva")  # por defecto primitiva
    
    row = [json.dumps(prediction, ensure_ascii=False), fecha_predecida, algoritmo, juego]
    # append (no header creation here; lo gestionas desde run_all.sh)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(row)
        
def build_last_sequence():
    df = load_processed_df()
    nums = df_to_numeros_list(df)
    last = nums[-WINDOW_K:]
    seq = [make_onehot_draw(draw) for draw in last]
    return np.stack(seq, axis=0).reshape(1, WINDOW_K, NUM_MAX)

def _load_model_pref():
    if os.path.isdir(MODEL_TF_DIR):
        try:
            return keras.models.load_model(MODEL_TF_DIR)
        except Exception as e:
            print("[predict_keras] fallo cargando SavedModel:", e)
    if os.path.exists(MODEL_KERAS_FILE):
        try:
            return keras.models.load_model(MODEL_KERAS_FILE)
        except Exception as e:
            print("[predict_keras] fallo cargando .keras:", e)
    if os.path.exists(MODEL_H5):
        try:
            return keras.models.load_model(MODEL_H5)
        except Exception as e:
            print("[predict_keras] fallo cargando .h5:", e)
    raise FileNotFoundError("No se encontró modelo (.tf/.keras/.h5). Entrena primero.")



def predict_next(top_k=6):
    model = _load_model_pref()
    X = build_last_sequence()
    probs = model.predict(X)[0]
    idx = np.argsort(probs)[::-1][:top_k] + 1
    return idx.tolist()
preds = predict_next()



if __name__ == '__main__':
    print('Keras sugerencia:', predict_next())
    preds = predict_next()
    juego_env = os.environ.get("JUEGO", "primitiva")
    compare_with_last(algorithm="keras",juego=juego_env )    
fecha = _today_madrid_iso()
append_prediction(preds, fecha, "keras", juego=juego_env)   # o "keras" en el otro fichero
