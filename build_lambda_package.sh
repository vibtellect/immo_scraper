#!/bin/bash
set -e

# 1. Im Projektverzeichnis starten
cd "$(dirname "$0")"

echo "Removing old venv if exists..."
rm -rf venv

echo "Creating new Python 3.9 virtual environment..."
python3.9 -m venv venv

echo "Activating virtual environment and installing dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r src/requirements.txt

echo "Preparing build directory..."
rm -rf build
mkdir build

echo "Packaging dependencies..."
cd venv/lib/python3.9/site-packages
zip -r9 ${OLDPWD}/build/lambda_function.zip .

echo "Adding lambda_function.py and config.py..."
cd $OLDPWD/src
zip -g ../build/lambda_function.zip lambda_function.py config.py

echo "Lambda deployment package is ready at build/lambda_function.zip"