#!/bin/bash
# Script to help switching between OAR2 <-> OAR3
#set -e

systemctl stop oar-server.service
apt-get -y remove oar-common
apt-get  update

apt-get -y install libappconfig-perl ibsort-naturally-perl php-pgsql php

apt-get -y install python3-sqlalchemy python3-sqlalchemy-utils python3-alembic \
        python3-flask python3-tabulate python3-click python3-zmq python3-requests \
        python3-simplejson python3-psutil python3-psycopg2

# TODO HIERARCHY_LABELS="resource_id,network_address,cpu,core" (autoconfig ?) cpu,core
grep -q '^HIERARCHY_LABELS' /etc/oar/oar.conf || echo 'HIERARCHY_LABELS="resource_id,network_address,core"' >> /etc/oar/oar.conf

mkdir -p /etc/oar/admission_rules.d/
grep -q '^ADMISSION_RULES_IN_FILES' /etc/oar/oar.conf || echo 'ADMISSION_RULES_IN_FILES="yes"' >> /etc/oar/oar.conf
grep -q '^JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD' /etc/oar/oar.conf || echo 'JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD="cpuset"' >> /etc/oar/oar.conf

grep -q '^TAKTUK_CMD' /etc/oar/oar.conf || echo ' TAKTUK_CMD="/usr/bin/taktuk -t 30 -s"' >> /etc/oar/oar.conf

OAR_SERVER=$(hostname)
sed -e "s/^\(SERVER_HOSTNAME\)=.*/\1=\"$OAR_SERVER\"/" -i /etc/oar/oar.conf

sed -e 's/^\(LOG_FILE\)=.*/\1="\/var\/log\/oar\.log"/' -i /etc/oar/oar.conf

dpkg -i /home/orichard/public/oar3-dpkgs/*.deb

# TODO oar-server oar-database --check
# oar-server -> systemd
#cp /home/orichard/oar-server /etc/init.d/
systemctl stop apache2
systemctl daemon-reload

echo Start oar-server.service
systemctl start oar-server.service
