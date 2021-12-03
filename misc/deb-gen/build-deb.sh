#!/usr/bin/env bash

DEBIAN_NAME=${1:-stretch}

DEBIAN_IMAGE=debian:$DEBIAN_NAME


DIR=$(dirname $0)

if [ ! -d build ]; then
    mkdir build
else
    echo "build directory exits, remove it before !"
    #exit 1
fi    

BUILD_DIR=$PWD/build
git clone --depth=50 --branch=debian/3.0 https://github.com/oar-team/oar3.git $BUILD_DIR/oar3

echo "debian version: $DEBIAN_NAME"

echo "build docker image"

cd $DIR/docker/
#echo docker build -t deb/$DEBIAN_NAME --build-arg DEBIAN_IMAGE=$DEBIAN_IMAGE .
docker build -t deb/$DEBIAN_NAME --build-arg DEBIAN_IMAGE=$DEBIAN_IMAGE . 

cd $BUILD_DIR

docker run -v $BUILD_DIR:/build  deb/$DEBIAN_NAME

echo "Debian packages in $BUILD_DIR"
