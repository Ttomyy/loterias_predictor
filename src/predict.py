# src/predict.py
import os
import joblib
import numpy as np
import pandas as pd

MODEL_FILE = os.path.join(os.path.dirname(__file__), "..", "models", "rf_multijoblib.pkl")
FEATURES_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "features.csv")

def load_latest_feature_row():
    df = pd.read_csv(FEATURES_CSV)
    # tomamos la última fila (más reciente)
    last = df.drop(columns=["fecha_target"], errors="ignore").iloc[-1]
    return last.values.reshape(1, -1)

def predict_next_combination(top_k=6):
    if not os.path.exists(MODEL_FILE):
        raise FileNotFoundError("Modelo no encontrado. Entrena primero.")
    clf = joblib.load(MODEL_FILE)
    X = load_latest_feature_row()
    probs = None
    # RandomForest en MultiOutputClassifier no ofrece predict_proba por defecto para multioutput en sklearn < 1.1
    # intentamos usar predict_proba por etiqueta si está disponible
    try:
        prob_list = [estimator.predict_proba(X)[:,1] for estimator in clf.estimators_]
        probs = np.array(prob_list).flatten()  # shape (49,)
    except Exception:
        # fallback: usar predict (0/1) y en su defecto la suma de estimators predict
        preds = clf.predict(X).flatten()
        # si preds es binario, devolvemos los que tengan 1; si hay más de top_k, priorizamos por orden
        if preds.sum() >= top_k:
            chosen = np.where(preds == 1)[0] + 1
            return chosen[:top_k].tolist()
        else:
            # como fallback simple, usamos la media de los árboles en el primer estimador si existe
            try:
                avg = np.zeros(49)
                for est in clf.estimators_:
                    try:
                        if hasattr(est, "predict_proba"):
                            avg += est.predict_proba(X)[:,1]
                    except Exception:
                        pass
                probs = avg / max(1, len(clf.estimators_))
            except Exception:
                probs = np.random.rand(49)

    # Si tenemos probs, elegimos los top_k indices
    idx = np.argsort(probs)[::-1][:top_k] + 1  # números 1..49
    return idx.tolist()

if __name__ == "__main__":
    print("Sugerencia (6 números):", predict_next_combination())
