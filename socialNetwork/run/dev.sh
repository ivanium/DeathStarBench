#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

IMAGE_NAME=socialnet_buildbase
CONTAINER_NAME=socialnet_buildbase

start_docker() {
    docker_running=$(docker ps --format '{{.Names}}' | grep socialnet_buildbase)
    if [[ ! $docker_running ]]
    then
        docker run -d -it --name ${CONTAINER_NAME} \
            -v ${ROOT_DIR}:${ROOT_DIR} \
            -v ${ROOT_DIR}/services:/services \
            -v ${ROOT_DIR}/config:/config \
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
