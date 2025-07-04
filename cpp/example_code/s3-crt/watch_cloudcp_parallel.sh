#!/bin/bash

# Arguments to cloudcp_parallel.sh
SRC_DIR="/bryck/testdata"
S3_URI="s3://crtbucket"
ENDPOINT="--endpoint-url https://10.10.10.155:9000"

# Enable core dumps
ulimit -c unlimited
echo "/tmp/core.%e.%p" | sudo tee /proc/sys/kernel/core_pattern > /dev/null
echo "Core dumps enabled. Pattern: $(cat /proc/sys/kernel/core_pattern)"

# Path to your script
CLOUDCP_SCRIPT="./cloudcp_parallel.sh"

# Interval between retries (optional)
SLEEP_INTERVAL=5

while true; do
    echo "==> Starting cloudcp_parallel.sh at $(date)"
    "$CLOUDCP_SCRIPT" "$SRC_DIR" "$S3_URI" "$ENDPOINT"

    EXIT_CODE=$?
    echo "==> Script exited with code: $EXIT_CODE at $(date)"

    if [ "$EXIT_CODE" -ne 0 ]; then
        echo "❌ Script crashed or failed, dumping info..."
    fi

    echo "==> Sleeping for $SLEEP_INTERVAL seconds before restarting"
    sleep "$SLEEP_INTERVAL"
done

