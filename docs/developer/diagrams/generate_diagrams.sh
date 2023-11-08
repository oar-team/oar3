#!/usr/bin/env bash

for file in *.pu; do
  plantuml $file -tpng
  plantuml $file -tsvg

  mv *.png ../../_static
  mv *.svg ../../_static
done