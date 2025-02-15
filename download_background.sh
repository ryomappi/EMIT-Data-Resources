#!/bin/bash

mkdir -p nohup_logs
LOGFILE="nohup_logs/get_data_with_geojson_$(date +'%Y%m%d_%H%M%S').log"
nohup python3 -u python/src/get_data_with_geojson.py --json data/dataset/geojsons > "$LOGFILE" 2>&1 &
