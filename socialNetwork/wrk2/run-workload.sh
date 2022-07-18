#!/bin/bash

NR_THD=48
NR_CONN=192
DUR=20

WORKLOAD=mixed-worload
ADDR=http://localhost:8080
API=


# Compose posts
compose_post() {
    echo Start composing posts...
    WORKLOAD=compose-post
    API=/wrk2-api/post/compose
    ./wrk -D exp -t ${NR_THD} -c ${NR_CONN} -d ${DUR} \
        -L -s ./scripts/social-network/${WORKLOAD}.lua \
        ${ADDR}${API} \
        -R 100000000
    echo Finish composing posts...
}

read_home_timelines() {
    echo Start reading home timelines...
    WORKLOAD=read-home-timeline
    API="/wrk2-api/home-timeline/read"
    ./wrk -D exp -t ${NR_THD} -c ${NR_CONN} -d ${DUR} \
        -L -s ./scripts/social-network/${WORKLOAD}.lua \
        ${ADDR}${API} \
        -R 100000000
    echo Finish reading home timelines...
}

read_user_timelines() {
    echo Start reading user timelines...
    WORKLOAD=read-user-timeline
    API="/wrk2-api/user-timeline/read"
    ./wrk -D exp -t ${NR_THD} -c ${NR_CONN} -d ${DUR} \
        -L -s ./scripts/social-network/${WORKLOAD}.lua \
        ${ADDR}${API} \
        -R 100000000
    echo Finish read user timelines...
}

compose_post

# read_home_timelines
