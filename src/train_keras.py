# src/train_keras.py
import os, shutil, numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
#from utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw
try:
    # cuando se ejecuta como paquete (python -m src.predict_keras)
    from src.utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw
except Exception:
    # cuando se ejecuta directamente (python src/predict_keras.py)
    from utils_ml import load_processed_df, df_to_numeros_list, make_onehot_draw

MODEL_DIR = os.path.join(os.path.dirname(__file__), '..', 'models')
MODEL_KERAS_FILE = os.path.join(MODEL_DIR, 'keras_lstm.keras')
MODEL_TF_DIR = os.path.join(MODEL_DIR, 'keras_lstm_tf')
WINDOW_K = 8
NUM_MAX = 49
SEED = 49

tf.random.set_seed(SEED)
np.random.seed(SEED)

def build_sequences(df, window_k=WINDOW_K):
    nums = df_to_numeros_list(df)
    X, y = [], []
    for i in range(window_k, len(nums)):
        seq = [make_onehot_draw(draw) for draw in nums[i-window_k:i]]
        X.append(np.stack(seq, axis=0).astype(np.float32))
        label = np.zeros(NUM_MAX, dtype=np.float32)
        for n in nums[i]:
            label[n-1] = 1.0
        y.append(label)
    if not X:
        return np.array([]), np.array([])
    return np.stack(X, axis=0), np.stack(y, axis=0)

def build_model(window_k=WINDOW_K, num_max=NUM_MAX):
    inp = keras.Input(shape=(window_k, num_max))
    x = layers.Masking()(inp)
    x = layers.LSTM(512)(x)
    x = layers.Dense(512, activation='relu')(x)
    out = layers.Dense(num_max, activation='sigmoid')(x)
    model = keras.Model(inputs=inp, outputs=out)
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

def train(epochs=49, batch_size=6):
    print("Cargando datos procesados...")
    df = load_processed_df()
    X, y = build_sequences(df)
    if X.size == 0:
        raise RuntimeError("No hay secuencias para entrenar. Ejecuta ETL y procesa datos primero.")
    split = int(0.8 * len(X))
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]
    model = build_model()
    os.makedirs(MODEL_DIR, exist_ok=True)
    callbacks = [
        keras.callbacks.EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True),
        keras.callbacks.ModelCheckpoint(os.path.join(MODEL_DIR, 'keras_lstm_best.keras'),
                                        monitor='val_loss', save_best_only=True)
    ]
    print(f"Entrenando Keras LSTM (epochs={epochs}) ...")
    model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=epochs, batch_size=batch_size, callbacks=callbacks)
    print("Guardando modelo nativo Keras (.keras):", MODEL_KERAS_FILE)

    # Guardar en formato nativo Keras (.keras)
    print("Guardando modelo en formato nativo Keras:", MODEL_KERAS_FILE)
    model.save(MODEL_KERAS_FILE)  # .keras es el formato recomendado en Keras 3

    # Guardar como SavedModel: usar model.export() si está disponible (Keras 3),
    # con tf.saved_model.save() como fallback razonable.
    print("Intentando exportar SavedModel en:", MODEL_TF_DIR)
    if os.path.exists(MODEL_TF_DIR):
        import shutil
        try:
            shutil.rmtree(MODEL_TF_DIR)
        except Exception:
            pass

    try:
        # Keras 3: preferir model.export (maneja internals Keras correctamente)
        if hasattr(model, "export"):
            model.export(MODEL_TF_DIR)
        else:
            # fallback (antiguas versiones): intentar saved_model
            import tensorflow as tf
            tf.saved_model.save(model, MODEL_TF_DIR)
        print("SavedModel guardado en:", MODEL_TF_DIR)
    except Exception as e:
        print("Error exportando SavedModel con model.export()/tf.saved_model.save():", e)
        # Re-lanzamos para que el proceso muestre la traza completa si quieres debug
        raise

    print("Entrenamiento y guardado completados.")
    return MODEL_KERAS_FILE, MODEL_TF_DIR

if __name__ == '__main__':
    train(epochs=50, batch_size=32)


