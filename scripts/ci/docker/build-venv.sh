#!/bin/bash
set -eux

python3 -m venv /venv
source /venv/bin/activate

pip install -U pip
pip install wheel

# Install poetry
curl -sSL https://install.python-poetry.org | python3 # Add poetry to path
export PATH="/root/.local/bin:$PATH"

poetry --version
poetry config virtualenvs.create false
poetry config --list

# Project initialization:
cd app && poetry install

find /venv -name "*.pyc" -delete
