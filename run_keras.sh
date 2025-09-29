#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"   # se mueve al directorio donde está run_keras.sh (la raíz del repo)

# Ejecuta ETL -> train keras -> predict keras
python -m src.etl
python -m src.train_keras
python -m src.predict_keras
