#!/bin/bash

export LC_ALL=C
export DEBIAN_FRONTEND=noninteractive
export WAIT_FOR_IT_VERSION=9995b721327eac7a88f0dce314ea074d5169634f

set -e
set -x

apt-get update
apt-get install -y libpq-dev make

# Install wait for it
curl -sSL https://raw.githubusercontent.com/vishnubob/wait-for-it/$WAIT_FOR_IT_VERSION/wait-for-it.sh > /usr/local/bin/wait-for-it
chmod +x /usr/local/bin/wait-for-it

# Cleanup
apt-get clean -y
apt-get autoclean -y
apt-get autoremove -y
apt-get purge -y --auto-remove

rm -f /etc/dpkg/dpkg.cfg.d/02apt-speedup
rm -rf /tmp/* /var/tmp/*
rm -rf /var/lib/apt/lists/*
rm -rf /root/.cache
rm -rf /var/cache/debconf/*-old
rm -rf /var/lib/apt/lists/*
