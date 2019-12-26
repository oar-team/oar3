#!/usr/bin/env bash
export ROOT_PROJECT="$(git rev-parse --show-toplevel)"

export COMPOSE_FILE=$ROOT_PROJECT/scripts/ci/docker/docker-compose.test.yml
export COMPOSE_PROJECT_NAME="$(basename "$ROOT_PROJECT")_ci"

cd $ROOT_PROJECT
docker-compose build
