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
    git clone https://github.com/oar-team/oar-lib.git
    pip install -e /home/docker/oar-lib
    pip install -e /home/docker/oar-kao
    ln -s /usr/local/bin/kamelot /usr/local/lib/oar/schedulers/kamelot
    ln -s /usr/local/bin/kao /usr/local/lib/oar/kao
    ln -s /usr/local/bin/fakekao /usr/local/lib/oar/fakekao
}

function install_base {
    pip install git+https://github.com/oar-team/oar-lib.git
    pip install git+https://github.com/oar-team/oar-kao.git
    ln -s /usr/local/bin/kamelot /usr/local/lib/oar/schedulers/kamelot
    ln -s /usr/local/bin/kao /usr/local/lib/oar/kao
    ln -s /usr/local/bin/fakekao /usr/local/lib/oar/fakekao
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
    systemctl stop oardocker-server
    echo 'META_SCHED_CMD="fakekao"' >> /etc/oar/oar.conf
    systemctl start oardocker-server
}

function meta_sched_kao {
    systemctl stop oardocker-server
    echo 'META_SCHED_CMD="/usr/local/bin/kao"' >> /etc/oar/oar.conf
    systemctl start oardocker-server
}


#default_cmds=(install_local_base meta_sched_kao kamelot_queue_default)
default_cmds=(install_base meta_sched_kao kamelot_queue_default)

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
