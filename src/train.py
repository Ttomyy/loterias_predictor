# src/train.py
import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, hamming_loss
import joblib

FEATURES_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "features.csv")
LABELS_NPY = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "labels.npy")
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
MODEL_FILE = os.path.join(MODEL_DIR, "rf_multijoblib.pkl")

def load_data():
    X = pd.read_csv(FEATURES_CSV)
    # quitamos columna fecha_target si existe
    if "fecha_target" in X.columns:
        X = X.drop(columns=["fecha_target"])
    y = np.load(LABELS_NPY)
    return X.values, y

def train_and_save(test_size=0.2, random_state=42):
    X, y = load_data()
    if len(X) == 0:
        raise RuntimeError("No hay datos para entrenar. Ejecuta ETL y features primero.")
    # split manteniendo orden temporal -> no mezclar (shuffle=False)
    split_idx = int((1 - test_size) * len(X))
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    # Clasificador multi-output (un RandomForest por etiqueta)
    base = RandomForestClassifier(n_estimators=100, n_jobs=-1, random_state=random_state)
    clf = MultiOutputClassifier(base, n_jobs=-1)
    print("Entrenando modelo... esto puede tardar un poco")
    clf.fit(X_train, y_train)

    # Evaluación básica
    y_pred = clf.predict(X_test)
    # f1 micro average across labels (multi-label)
    try:
        f1 = f1_score(y_test, y_pred, average="micro")
    except Exception:
        f1 = None
    ham = hamming_loss(y_test, y_pred)

    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(clf, MODEL_FILE)
    print("Modelo guardado en:", MODEL_FILE)
    print("Eval: f1_micro=", f1, " hamming_loss=", ham)
    return MODEL_FILE

if __name__ == "__main__":
    train_and_save()
