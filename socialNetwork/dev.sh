#!/bin/bash

IMAGE_NAME=socialnet_buildbase
CONTAINER_NAME=socialnet_buildbase

start_docker() {
    docker_running=$(docker ps -q)
    if [[ ! $docker_running ]]
    then
        docker run -d -it --name ${CONTAINER_NAME} \
            -v $(pwd):/mnt/ssd/yifan/code/DeathStarBench/socialNetwork \
            ${IMAGE_NAME} /bin/bash
        # docker run -d -it --name ${CONTAINER_NAME} \
        #     -v $(pwd)/services:/services \
        #     -v $(pwd)/config:/config \
        #     ${IMAGE_NAME} /bin/bash
    fi
}

into_docker() {
    docker exec -it ${CONTAINER_NAME} /bin/bash
}

if [[ ! $1 ]] || [[ $1 == start ]]
then
    start_docker
    into_docker
elif [[ $1 == stop ]]
then
    docker stop ${CONTAINER_NAME}
    docker rm ${CONTAINER_NAME}
fi
