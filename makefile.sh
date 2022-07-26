#!/bin/bash
set -ex

pwd=$(pwd)
echo "Build started from the path: $pwd"
rm -rf ./dist/
mkdir -p ./dist/pyspark_app
mkdir -p ./tmp/dependencies
# copy required files and zip for pyspark
cp -r ./configs ./tmp/dependencies/
cd ./tmp/dependencies && zip -r ../../dist/pyspark_app/dependencies.zip . && cd "$pwd"
rm -r ./tmp
echo "Build success"
