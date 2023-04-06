#!/bin/bash

export SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

# cp ${ROOT_DIR}/build/src/Client/Client ${ROOT_DIR}/services/Client
echo "docker exec -it mediamicroservices_yifan-client_1 /services/Client $1"
docker exec -it mediamicroservices_yifan-client_1 /services/Client $1
# docker exec -it socialnetwork-yifan-client-1 /bin/bash
