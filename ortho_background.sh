#!/bin/bash

mkdir -p nohup_logs
LOGFILE="nohup_logs/ortho_dataset_$(date +'%Y%m%d_%H%M%S').log"
nohup python3 -u python/src/ortho_dataset.py > "$LOGFILE" 2>&1 &
