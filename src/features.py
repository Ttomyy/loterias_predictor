# src/features.py
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

# Paths
import os
BASE = os.path.join(os.path.dirname(__file__), '..')
PREFIX = os.environ.get('JUEGO', 'primitiva')
PROCESSED_CSV = os.path.join(BASE, 'data', 'processed', f"{PREFIX}_processed.csv")
OUT_FEATURES = os.path.join(BASE, "data", "processed", "features.csv")
OUT_LABELS = os.path.join(BASE, "data", "processed", "labels.npy")

# Parameters
WINDOW_K = 10  # cuántos sorteos previos usar para features
NUM_MAX = 49

def build_features(window_k=WINDOW_K):
    df = pd.read_csv(PROCESSED_CSV, parse_dates=["fecha"], dayfirst=True)
    # construimos lista de listas con las 6 numeros por fila
    numeros_list = []
    for _, row in df.iterrows():
        nums = []
        for col in ["n1","n2","n3","n4","n5","n6"]:
            v = row.get(col)
            if pd.isna(v):
                continue
            nums.append(int(v))
        numeros_list.append(nums)

    # preparaciones
    rows_features = []
    rows_labels = []
    dates = []

    # for i from window_k to len-1, create sample using prior window_k draws to predict draw i
    for i in range(window_k, len(numeros_list)):
        prev_window = numeros_list[i-window_k:i]  # list of lists
        # Feature 1: frequency counts of each number in previous K draws
        counts = np.zeros(NUM_MAX, dtype=int)
        for draw in prev_window:
            for n in draw:
                counts[n-1] += 1
        # Feature 2: indicator vector of last draw (previous one)
        last_draw = np.zeros(NUM_MAX, dtype=int)
        for n in prev_window[-1]:
            last_draw[n-1] = 1
        # Additional feature: position index (normalized)
        idx_norm = np.array([i / max(1, len(numeros_list))])

        feat = np.concatenate([counts, last_draw, idx_norm])
        rows_features.append(feat)

        # Label (multi-hot vector length 49) for current draw numeros_list[i]
        label = np.zeros(NUM_MAX, dtype=int)
        for n in numeros_list[i]:
            label[n-1] = 1
        rows_labels.append(label)
        dates.append(df.iloc[i]["fecha"])

    X = np.vstack(rows_features)
    y = np.vstack(rows_labels)

    # Guardar X como CSV con columnas prefijadas
    cols = [f"cnt_{n}" for n in range(1, NUM_MAX+1)] + [f"last_{n}" for n in range(1, NUM_MAX+1)] + ["idx_norm"]
    dfX = pd.DataFrame(X, columns=cols)
    dfX["fecha_target"] = dates
    os.makedirs(os.path.dirname(OUT_FEATURES), exist_ok=True)
    dfX.to_csv(OUT_FEATURES, index=False)
    np.save(OUT_LABELS, y)
    print("Features guardadas en:", OUT_FEATURES)
    print("Labels guardados en:", OUT_LABELS)
    return OUT_FEATURES, OUT_LABELS

if __name__ == "__main__":
    build_features()
