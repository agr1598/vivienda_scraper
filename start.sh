#!/usr/bin/env bash
set -e

# 1) Crear la carpeta de salida (si no existe)
mkdir -p "$PWD/output"

# 2) Ejecutar el scraper
python main.py
