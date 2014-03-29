#!/bin/bash

if [ $# != 1 ]; then
    echo usage: remap.sh recordings-folder
    exit 1
fi

FOLDER=$1

