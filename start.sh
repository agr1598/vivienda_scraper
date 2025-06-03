#!/usr/bin/env bash

# 1) Actualizar repositorios e instalar Firefox (versión ESR estable)
apt-get update -y
apt-get install -y firefox-esr

# 2) Descargar e instalar la última versión de geckodriver
GECKODRIVER_VERSION=$(curl -s https://api.github.com/repos/mozilla/geckodriver/releases/latest \
                    | grep 'tag_name' \
                    | cut -d\" -f4)
wget "https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VERSION}/geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz"
tar -xzf "geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz"
mv geckodriver /usr/local/bin/
chmod +x /usr/local/bin/geckodriver

# 3) Crear carpeta de salida si no existe
mkdir -p "$PWD/output"

# 4) Ejecutar el script principal
python main.py
