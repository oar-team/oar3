#!/usr/bin/env bash

# Simple script that checks if the code is correctly
# formated using the trinity of tools: isort, black and flake8.
# The tools are configured with file lying in the oar repository.
# - flake8: .flake8
# - isort and black: pyproject.toml

# Exit on error
set -e

echo "-- Check imports"
isort . --check-only --diff

echo "-- Check code formatting"
black . --check --diff

echo "-- Static code check"
flake8 .
