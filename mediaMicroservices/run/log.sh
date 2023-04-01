#!/bin/bash
docker logs -f `docker ps | grep $1 | cut -d' ' -f1`