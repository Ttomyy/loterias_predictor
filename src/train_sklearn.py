import os
import joblib
import numpy as np
import pandas as pd
from sklearn.multioutput import MultiOutputClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, hamming_loss
#from utils_ml import load_processed_df, df_to_numeros_list
try:
    # cuando se ejecuta como paquete (python -m src.predict_keras)
    from src.utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw
except Exception:
    # cuando se ejecuta directamente (python src/predict_keras.py)
    from utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw

MODEL_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
MODEL_FILE = os.path.join(MODEL_DIR, 'rf_multijoblib.pkl')
WINDOW_K = 8
NUM_MAX = 49


def build_X_y(df, window_k=WINDOW_K):
    numeros_list = df_to_numeros_list(df)
    X_rows = []
    y_rows = []
    for i in range(window_k, len(numeros_list)):
        prev = numeros_list[i-window_k:i]
        counts = np.zeros(NUM_MAX, dtype=int)
        for draw in prev:
            for n in draw:
                counts[n-1] += 1
        last = np.zeros(NUM_MAX, dtype=int)
        for n in prev[-1]:
            last[n-1] = 1
        idx_norm = np.array([i / max(1, len(numeros_list))])
        feat = np.concatenate([counts, last, idx_norm])
        X_rows.append(feat)
        label = np.zeros(NUM_MAX, dtype=int)
        for n in numeros_list[i]:
            label[n-1] = 1
        y_rows.append(label)
    X = np.vstack(X_rows)
    y = np.vstack(y_rows)
    return X, y


def train():
    df = load_processed_df()
    X, y = build_X_y(df)
    split = int(0.8 * len(X))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    clf = MultiOutputClassifier(RandomForestClassifier(n_estimators=500, n_jobs=-1, random_state=42), n_jobs=-1)
    print('Entrenando SKLearn RandomForest...')
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    try:
        f1 = f1_score(y_test, y_pred, average='micro')
    except Exception:
        f1 = None
    ham = hamming_loss(y_test, y_pred)

    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(clf, MODEL_FILE)
    print('SKLearn Modelo guardado en', MODEL_FILE)
    print('Eval f1_micro=', f1, 'hamming_loss=', ham)


if __name__ == '__main__':
    train()