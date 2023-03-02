#!/bin/bash

db_containers=$(cat db_containers.txt)

for c in $db_containers
do
    echo $c
    mkdir -p dbs/$c
    docker cp $c:/data/db dbs/$c/
done
