./run/down.sh
# sudo rm -rf dbs
# sudo cp -r db_backups/after_register_users/dbs .
# sudo rm -rf dbs/mediamicroservices_review-storage-mongodb_1
# sudo mkdir dbs/mediamicroservices_review-storage-mongodb_1
cp datasets/tmdb/filtered_large_names.json config/names.json
cp datasets/tmdb/filtered_large_ids.json config/ids.json
./run/up.sh
echo "docker exec -it mediamicroservices_yifan-client_1 /services/Client 0"
docker exec -it mediamicroservices_yifan-client_1 /services/Client 0
