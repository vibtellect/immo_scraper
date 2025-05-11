#!/bin/bash
# Script zum Erstellen des Lambda-Pakets für den Node.js-basierten Bazaraki Scraper

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/src"
BUILD_DIR="${SCRIPT_DIR}/build_nodejs"
OUT_FILE="${SCRIPT_DIR}/lambda_function_nodejs.zip"

echo "Erstelle Lambda-Paket für den Bazaraki Node.js Scraper..."

# Build-Verzeichnis erstellen und bereinigen
mkdir -p "${BUILD_DIR}"
rm -rf "${BUILD_DIR:?}"/*
rm -f "${OUT_FILE}"

# JavaScript-Dateien und package.json kopieren
cp "${SRC_DIR}/bazaraki_lambda_scraper.js" "${BUILD_DIR}/index.js"
cp "${SRC_DIR}/package.json" "${BUILD_DIR}/"

# Nach build_dir wechseln
cd "${BUILD_DIR}"

# Node.js-Abhängigkeiten installieren (nur Produktionsabhängigkeiten)
echo "Installiere Node.js-Abhängigkeiten..."
npm install --production

# Pakete bereinigen, um Größe zu reduzieren
echo "Optimiere Paketgröße..."
find node_modules -type d -name "test" -o -name "tests" -o -name "example" -o -name "examples" -o -name "docs" | xargs rm -rf
find node_modules -name "*.md" -o -name "*.ts" -o -name "*.map" -o -name "LICENSE*" | xargs rm -f

# Paket erstellen
echo "Erstelle ZIP-Paket..."
zip -r "${OUT_FILE}" .

# Fertig
echo "Lambda-Paket erstellt: ${OUT_FILE}"
echo "Paketgröße: $(du -h "${OUT_FILE}" | cut -f1)"
