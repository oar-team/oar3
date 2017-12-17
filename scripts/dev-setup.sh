#!/usr/bin/env bash


RESTORE='\033[0m'

RED='\033[00;31m'
GREEN='\033[00;32m'
YELLOW='\033[00;33m'
BLUE='\033[00;34m'
PURPLE='\033[00;35m'
CYAN='\033[00;36m'
LIGHTGRAY='\033[00;37m'


function setup_server {
    echo "setup_server"
    sudo systemctl stop oardocker-server
    #resource creations
    sudo systemctl start oardocker-resources
    cd /data
    sudo pip install -e .
    sudo echo 'META_SCHED_CMD="/usr/local/bin/kao"' >> /etc/oar/oar.conf
    sudo su - oar -c "psql oar -c 'truncate admission_rules'"
    
    #export OARCONFFILE='/etc/oar/oar.conf'; export OARDO_USER='oar';    
    }

function setup_frontend {
    echo "setup_frontend"
    cd /data
    sudo pip install -e .
    sudo mv /usr/local/lib/oar/oarsub /usr/local/lib/oar/oarsub2
    sudo ln -s /usr/local/bin/oarsub3 /usr/local/lib/oar/oarsub
}

host=`hostname`
echo -e ${GREEN}$host${RESTORE}

if [ $# -eq 0 ]
then
    setup_$host
else
    echo $1
fi
