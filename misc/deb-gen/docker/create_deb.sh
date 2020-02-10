#!/usr/bin/env bash
set -e
cd /build/oar3
dpkg-buildpackage -b -rfakeroot -us -uc
exit
