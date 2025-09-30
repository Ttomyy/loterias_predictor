#!/usr/bin/env bash
set -euo pipefail
trap 'echo "❌ ERROR en línea $LINENO (comando: $BASH_COMMAND)" >> logs/status.log; exit 1' ERR

# run_all.sh - Ejecuta pipeline completo para un juego concreto
# Uso: ./run_all.sh primitiva   o   ./run_all.sh bonoloto

cd "$(dirname "$0")"


# -------------------------
# Elegir juego (por argumento)
# -------------------------
JUEGO="${1:-primitiva}"   # por defecto primitiva si no se pasa argumento
case "$JUEGO" in
  primitiva)
    WHICH="1"
    ;;
  bonoloto)
    WHICH="2"
    ;;
  *)
    echo "Juego no válido: $JUEGO"
    exit 1
    ;;
esac

echo "Juego seleccionado: $JUEGO (WHICH=$WHICH)"
export JUEGO
export PREFIX="$JUEGO"
PYTHON="python"

# -------------------------
# Preparar ficheros
# -------------------------
PRED_FILE="predicciones"
if [ ! -f "$PRED_FILE" ]; then
  printf '%s\n' '"prediccion","fecha_predecida","algoritmo","juego"' > "$PRED_FILE"
  echo "Creado fichero de predicciones: $PRED_FILE"
fi

mkdir -p data
INFO_FILE="data/infocompare.txt"
printf 'Informe de comparaciones\n' > "$INFO_FILE"

# -------------------------
# Pipeline
# -------------------------
echo "=== Iniciando pipeline completo ==="

echo "[1/8] Scraper y cargar datos en MongoDB..."
$PYTHON -m src.scraper_mongo --which "$WHICH" --save --name "$JUEGO" || true

echo "[2/8] Ejecutando ETL..."
$PYTHON -m src.etl --which "$WHICH" --prefix "$JUEGO" --to-mongo

echo "[3/8] Generando features..."
$PYTHON -m src.features --prefix "$JUEGO"
$PYTHON -m src.utils_ml

echo "[4/8] Entrenando modelo SKLearn..."
$PYTHON -m src.train_sklearn

echo "[5/8] Predicción SKLearn..."
$PYTHON -m src.predict_sklearn

echo "[6/8] Entrenando modelo Keras..."
$PYTHON -m src.train_keras

echo "[7/8] Predicción Keras..."
$PYTHON -m src.predict_keras

echo "[8/8] Resumen predicciones (últimas líneas):"
tail -n 10 "$PRED_FILE" || true

echo "[9/8] Enviando email con resultados..."
$PYTHON -m src.send_email --prefix "$JUEGO"

echo "=== Pipeline finalizado para $JUEGO ==="
echo "✅ OK $JUEGO $(date)" >> logs/status.log
