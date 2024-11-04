#!/bin/bash

rm -rf ./build/*

mkdir -p ./build

cp -R ./config ./build

go build -o ./build/duplicheck.bin

chmod +x ./build/duplicheck.bin