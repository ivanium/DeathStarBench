#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

apps=( CastInfoService ComposeReviewService MovieIdService MovieReviewService RatingService ReviewStorageService TextService UniqueIdService UserReviewService UserService PlotService MovieInfoService PageService)

for app in ${apps[@]}
do
#     echo $app
    cp ${ROOT_DIR}/build/src/$app/$app ${ROOT_DIR}/services/
done