#!/bin/bash

# oardocker start -n 3  -v /home/auguste/prog/oar-kao:/home/docker/oar-kao
# oardocker exec --user root server /home/docker/oar-kao/scripts/kao_setup.sh

RESTORE='\033[0m'

RED='\033[00;31m'
GREEN='\033[00;32m'
YELLOW='\033[00;33m'
BLUE='\033[00;34m'
PURPLE='\033[00;35m'
CYAN='\033[00;36m'
LIGHTGRAY='\033[00;37m'

function install_local_base {
    apt-get install -y python-dev # TODO move to oar-docker
    pip install git+https://github.com/oar-team/python-oar-lib.git

    cd /home/docker/oar-kao; pip install .
    ln -s /home/docker/oar-kao/oar_kao/kamelot.py /usr/local/lib/oar/schedulers/kamelot
    ln -s /home/docker/oar-kao/oar_kao/kao.py /usr/local/lib/oar/kao
    ln -s /home/docker/oar-kao/oar_kao/fakekao /usr/local/lib/oar/fakekao
    # ln -s /home/docker/oar-kao/oar_kao/judas_notify_user.pl /usr/local/lib/oar/judas_notify_user.pl
}

function install_base {
    apt-get install -y python-dev # TODO move to oar-docker
    pip install git+https://github.com/oar-team/oar-lib.git
    pip install git+https://github.com/oar-team/oar-kao.git
    ln -s /home/docker/oar-kao/oar_kao/kamelot.py /usr/local/lib/oar/schedulers/kamelot
    ln -s /home/docker/oar-kao/oar_kao/kao.py /usr/local/lib/oar/kao
    ln -s /home/docker/oar-kao/oar_kao/fakekao /usr/local/lib/oar/fakekao
    # ln -s /home/docker/oar-kao/oar_kao/judas_notify_user.pl /usr/local/lib/oar/judas_notify_user.pl
}

function redis {
    apt-get install -y redis-server python-redis
    sudo /etc/init.d/redis-server start
}


function kamelot_queue_default {
    oarnotify -D
    oarnotify --remove-queue "default"
    oarnotify --add-queue "default,3,kamelot"
    oarnotify -E
}

function meta_sched_fakekao {
    service oar-server stop
    echo 'META_SCHED_CMD="fakekao"' >> /etc/oar/oar.conf
    service oar-server start
}

function meta_sched_kao {
    service oar-server stop
    echo 'META_SCHED_CMD="kao"' >> /etc/oar/oar.conf
    service oar-server start
}


default_cmds=(install_local_base meta_sched_kao kamelot_queue_default)
#default_cmds=(install_base kamelot_queue_default)

if [ $# -eq 0 ]
then
    echo "kao_up: execute default setup commands"
    cmds=("${default_cmds[@]}")
else
    cmds=("$@")
fi

for cmd in "${cmds[@]}"
do
    echo -e ${GREEN}$cmd${RESTORE}
    $cmd
done
