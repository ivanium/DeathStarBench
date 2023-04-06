#!/bin/bash

./run/down.sh
./run/cp_services.sh
./run/up.sh
python3 scripts/write_movie_info.py -c datasets/tmdb/casts.json -m datasets/tmdb/movies.json && scripts/register_users.sh && scripts/register_movies.sh
cd wrk2
make
./wrk -t2 -c10 -d30s -L -s ./scripts/media-microservices/compose-review.lua http://localhost:8080/wrk2-api/review/compose -R2000
cd ..
