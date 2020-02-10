#!/bin/bash
set -e

python3 -m venv /venv

cd /app
source /venv/bin/activate

pip install -U pip

for filename in `ls -1 /app/requirements/ | grep -v dev`; do
    pip install -r /app/requirements/$filename
done

rm -rf /root/.cache

find /app -name "*.pyc" -delete
find /venv -name "*.pyc" -delete
