#!/usr/bin/env bash
set -e
cd /root/google_ozon_prices

# load .env into environment
set -a
source .env
set +a

source .venv/bin/activate
python sync.py
