#!/bin/bash
echo "SKEDULER Worker — starting container bootstrap"

if [ -f /opt/spcs/stage/app.tar.gz ]; then
    echo "Loading app code from mounted stage..."
    cp -f /opt/spcs/stage/app.tar.gz /opt/spcs/
    cd /opt/spcs/
    tar -zxvf app.tar.gz
else
    echo "No stage mount found, creating dummy worker"
    mkdir -p /opt/spcs/app
    echo 'print("No code - no good!")' > /opt/spcs/app/worker.py
fi

cd /opt/spcs/app
exec python worker.py
