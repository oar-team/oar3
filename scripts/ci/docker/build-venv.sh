#!/bin/bash
set -eux

python3 -m venv /venv
source /venv/bin/activate

pip install -U pip
pip install wheel
pip --no-cache-dir install poetry

# Project initialization:
cd app && POETRY_VIRTUALENVS_CREATE=false poetry install

find /venv -name "*.pyc" -delete
