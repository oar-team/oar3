#!/usr/bin/env bash

set -eux

: ${GIT_CLONE:=1}
: ${GIT_REMOTE_CLONE:=1}
: ${BRANCH_NAME:="debian/3.0"}

DEBIAN_NAME=${1:-bookworm}
#DEBIAN_NAME=${1:-sid}

DEBIAN_IMAGE=debian:$DEBIAN_NAME


DIR=$(dirname $0)

if [ ! -d build ]; then
    mkdir build
else
    echo "build directory exits, remove it before !"
    read -p "Press Y or y to continue ? " -n 1 -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]
       then
           exit 1
    fi
fi

BUILD_DIR=$PWD/build

if [ $GIT_CLONE == 1 ]; then
    if [ $GIT_REMOTE_CLONE == 1 ]; then
        echo "Clone oar3"
        git clone --depth=50 --branch=${BRANCH_NAME} https://github.com/oar-team/oar3.git $BUILD_DIR/oar3
    else
        echo "Work locally"
        git clone --branch=${BRANCH_NAME} ../.. $BUILD_DIR/oar3
        cp setup.py setup.cfg  $BUILD_DIR/oar3
    fi
fi

echo "debian version: $DEBIAN_NAME"

echo "build docker image"

cd $DIR/docker/
#echo docker build -t deb/$DEBIAN_NAME --build-arg DEBIAN_IMAGE=$DEBIAN_IMAGE .
docker build -t deb/$DEBIAN_NAME --build-arg DEBIAN_IMAGE=$DEBIAN_IMAGE .

cd $BUILD_DIR

echo docker run -v $BUILD_DIR:/build  deb/$DEBIAN_NAME
docker run -v $BUILD_DIR:/build  deb/$DEBIAN_NAME

echo "Debian packages in $BUILD_DIR"
