#!/usr/bin/env bash
set -e
# Ejecuta ETL -> features -> train (sklearn) -> predict
python -m src.scraper_mongo
python -m src.features
python -m src.train_sklearn
python -m src.predict_sklearn
