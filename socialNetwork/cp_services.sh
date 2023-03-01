#!/bin/bash

apps=( MediaService UrlShortenService PostStorageService UserMentionService ComposePostService SocialGraphService UserService HomeTimelineService TextService UserTimelineService UniqueIdService Client )

for app in ${apps[@]}
do
#     echo $app
    cp build/src/$app/$app services/
done