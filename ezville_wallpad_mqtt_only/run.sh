#!/bin/sh

PY_DIR=/pyezville
PY_FILE="ezville.py"
DEV_FILE="ezville_devinfo.json"

# start server
echo "[Info] Start ezville_wallpad_mqtt_only.."

python -u $PY_DIR/$PY_FILE
