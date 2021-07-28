#!/usr/bin/env bash
export ROOT_PROJECT="$(git rev-parse --show-toplevel)"

export COMPOSE_FILE=$ROOT_PROJECT/scripts/ci/docker/docker-compose.test.yml
export COMPOSE_PROJECT_NAME="$(basename "$ROOT_PROJECT")_ci"

export PYTHON_IMAGE=${PYTHON_IMAGE:-python:3.7}
export POSTGRES_IMAGE=${POSTGRES_IMAGE:-postgres:11}

echo "-------------------------------------------------------------------------------"
echo ""
echo "              Python : ${PYTHON_IMAGE}"
echo "          Postgresql : ${POSTGRES_IMAGE}"
echo ""
echo "-------------------------------------------------------------------------------"

docker_compose() {
    cd $ROOT_PROJECT
    docker-compose "$@"
}

set -e

docker_compose build
docker_compose down -v --remove-orphans 2> /dev/null
if [ $# -eq 0 ]; then
    docker_compose run --rm app py.test --cov=oar tests --cov-report=xml
else
    docker_compose run --rm app py.test --cov=oar "$@"
fi
docker_compose down -v --remove-orphans 2> /dev/null
