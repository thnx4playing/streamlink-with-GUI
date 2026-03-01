#!/bin/bash

DISPLAY_NUM=99

# Start Xvfb
Xvfb :${DISPLAY_NUM} -screen 0 1920x1080x24 -ac &
XVFB_PID=$!
export DISPLAY=:${DISPLAY_NUM}

# Wait for Xvfb to initialize
sleep 2

python app.py
