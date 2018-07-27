#!/bin/bash
# Script to help switching between OAR2 <-> OAR3
set -e

systemctl stop oar-server.service
apt-get -y remove oar-common
apt-get -y install python3-sqlalchemy python3-sqlalchemy-utils python3-alembic \
        python3-flask python3-tabulate python3-click python3-zmq python3-requests \
        python3-simplejson python3-psutil python3-psycopg2

update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1
update-alternatives --install /usr/bin/python python /usr/bin/python3.5 2

sed -e 's/Almighty/almighty/' -i /etc/init.d/oar-server
grep -q '^HIERARCHY_LABELS' /etc/oar/oar.conf || echo 'HIERARCHY_LABELS="resource_id,network_address,cpu,core"' >> /etc/oar/oar.conf

dpkg -i /home/orichard/public/oar3-dpkgs/*.deb
