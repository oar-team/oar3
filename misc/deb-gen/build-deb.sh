#!/usr/bin/env bash

: ${GIT_CLONE:=1}
: ${GIT_REMOTE_CLONE:=1}

DEBIAN_NAME=${1:-bullseye}
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

if [ $GIT_CLONE == 1]; then 
    if [ $GIT_REMOTE_CLONE == 1]; then 
        git clone --depth=50 --branch=debian/3.0 https://github.com/oar-team/oar3.git $BUILD_DIR/oar3
    else
        git clone --branch=debian/3.0 ../.. $BUILD_DIR/oar3
        cp setup.py setup.cfg  $BUILD_DIR/oar3/
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
