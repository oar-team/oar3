#!/usr/bin/env bash

set -e 

echo "-- Check imports"
isort . --check-only --diff

echo "-- Check code formatting"
black . --check --diff 