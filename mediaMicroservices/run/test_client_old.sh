./run/down.sh
cp datasets/tmdb/names_1_300.json config/names.json
cp datasets/tmdb/ids_1_300.json config/ids.json
sudo rm -rf dbs
mkdir dbs
./run/persist_dbs.sh
./run/up.sh
python3 scripts/write_movie_info.py -c datasets/tmdb/casts_1_300.json -m datasets/tmdb/movies_1_300.json && scripts/register_users.sh && scripts/register_movies.sh
echo "docker exec -it mediamicroservices_yifan-client_1 /services/Client $1 $2"
docker exec -it mediamicroservices_yifan-client_1 /services/Client $1 $2
