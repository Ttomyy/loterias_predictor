# scripts/inspect_h5.py
import h5py
import os
fpath = os.path.join("models", "keras_lstm.h5")
if not os.path.exists(fpath):
    raise SystemExit(f"No existe {fpath}")
f = h5py.File(fpath, "r")
def walk(g, prefix=""):
    for k in g:
        print(prefix + str(k))
        try:
            walk(g[k], prefix + "  ")
        except Exception:
            pass

print("Contenido del HDF5 (primeros nodos):")
walk(f)
# imprimir layer_names si existe
try:
    layer_names = list(f["model"].attrs.get("layer_names", []))
    print("\nlayer_names (raw):", layer_names)
except Exception:
    pass
f.close()
