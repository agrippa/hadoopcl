#!/bin/bash

if [ $# -lt 1 ]; then
    echo usage: remap.sh recordings-folder
    exit 1
fi

FOLDER=$1

python remap-recordings.py ${FOLDER}/recordings.saved ${FOLDER}/recordings.saved.remapped ${@:2:3}
python remap-launches.py ${FOLDER}/launches.saved ${FOLDER}/launches.saved.remapped ${@:2:3}

mv ${FOLDER}/recordings.saved ${FOLDER}/recordings.saved.backup
mv ${FOLDER}/launches.saved ${FOLDER}/launches.saved.backup

mv ${FOLDER}/recordings.saved.remapped ${FOLDER}/recordings.saved
mv ${FOLDER}/launches.saved.remapped ${FOLDER}/launches.saved
