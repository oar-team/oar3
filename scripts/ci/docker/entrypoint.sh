#!/usr/bin/env bash
set -e

CONTAINER_GID=$(stat -c '%g' /app)
CONTAINER_UID=$(stat -c '%u' /app)

groupadd -g $CONTAINER_GID oar || true
useradd -u $CONTAINER_UID -r -g oar -s /bin/bash -c "OAR User" -m oar || true
export USER=$(getent passwd $CONTAINER_UID | cut -d: -f1)


mkdir -p /etc/oar
touch /etc/oar/oar.conf

chown $CONTAINER_UID:$CONTAINER_GID /etc/oar

wait-for-it -t 120 ${POSTGRES_HOST}:5432

setpriv --reuid=$CONTAINER_UID --regid=$CONTAINER_GID --init-groups "$@"
