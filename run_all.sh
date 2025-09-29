#!/usr/bin/env bash
set -euo pipefail

# run_all.sh - Ejecuta pipeline completo: ETL, features, train/predict sklearn, train/predict keras
# Uso: ./run_all.sh

# Nos situamos en la raíz del repo (donde está este script)
cd "$(dirname "$0")"



# -------------------------
# Elegir juego (interactive)
# -------------------------
echo "Selecciona el juego a procesar:"
echo "  1) primitiva"
echo "  2) bonoloto"
read -r -p "Elige (1/2) [1]: " choice
choice="${choice:-1}"

case "$choice" in
  1)
    export JUEGO="primitiva"
    WHICH="1"
    ;;
  2)
    export JUEGO="bonoloto"
    WHICH="2"
    
    ;;
  *)
    echo "Opción no válida, se usará 'primitiva' por defecto."
    export JUEGO="primitiva"
    WHICH="1"
    ;;
esac

echo "Juego seleccionado: $JUEGO (WHICH=$WHICH)"
echo

export PREFIX="$JUEGO"  # para enviar email al final
echo "PREFIX para email: $PREFIX"
echo








PRED_FILE="predicciones"   # fichero solicitado (sin extensión)
# Crear fichero de predicciones con cabecera si no existe
if [ ! -f "$PRED_FILE" ]; then
  printf '%s\n' '"prediccion","fecha_predecida","algoritmo","juego"' > "$PRED_FILE"
  echo "Creado fichero de predicciones: $PRED_FILE"
else
  echo "Fichero de predicciones ya existe: $PRED_FILE"
fi
# asegurar carpeta data y fichero limpio por ejecución
INFO_FILE="data/infocompare.txt"
printf 'Informe de comparaciones\n' > "$INFO_FILE"

 
echo "=== Iniciando pipeline completo ==="
echo

echo "ejecuicion previa scraper y cargar datos en MongoDB (si no se ha hecho ya)"
python -m src.scraper_mongo --which "$WHICH" --save --name "${JUEGO}" || true
echo



echo "[1/8] Ejecutando ETL (extraer desde Mongo y generar CSV)..."
python -m src.etl --which "$WHICH" --prefix "$JUEGO"  --to-mongo 

echo "[2/8] Generando features a partir del CSV procesado..."
python -m src.features --prefix "$JUEGO"
python -m src.utils_ml



echo "[3/8] Entrenando modelo SKLearn..."
python -m src.train_sklearn

echo "[4/8] Ejecutando predicción SKLearn (se añadirá al fichero de predicciones)..."
python -m src.predict_sklearn

echo "[5/8] Entrenando modelo Keras (LSTM)..."
python -m src.train_keras

echo "[6/8] Ejecutando predicción Keras (se añadirá al fichero de predicciones)..."
python -m src.predict_keras

echo
echo "[7/8] (Opcional) Comparación/estadísticas adicionales:"
# Si tienes un script de comparación global, descomenta o ajusta la siguiente línea:
# python -m src.compare_results


echo "[8/8] Resumen: últimas líneas del fichero de predicciones:"
# Mostrar las últimas 10 líneas para verificar
tail -n 10 "$PRED_FILE" || true

echo "[9/8] Enviando email con la última predicción generada..."
python -m src.send_email --prefix "$JUEGO"
echo
echo "=== Pipeline completo finalizado ==="
