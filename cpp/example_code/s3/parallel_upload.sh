#!/bin/bash

# Usage: ./parallel_upload.sh <bucket-name> <source-dir> [concurrency]
# Example: ./parallel_upload.sh mybucket ./data 16

set -euo pipefail
set -x

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <bucket-name> <source-dir> [concurrency]"
    exit 1
fi

BUCKET_NAME="$1"
SRC_DIR="$2"
CONCURRENCY="${3:-$(nproc)}"  # Default to number of CPU cores

# Export the binary path if needed
PUT_OBJECT_BIN="./run_put_object_dio_single"

export PUT_OBJECT_BIN
export BUCKET_NAME
export SRC_DIR

find "$SRC_DIR" -type f | parallel -j "$CONCURRENCY" --env PUT_OBJECT_BIN,BUCKET_NAME,SRC_DIR '
    FILE={}
    REL_PATH="${FILE#$SRC_DIR/}"
    echo "Uploading $FILE → s3://$BUCKET_NAME/$REL_PATH"
    "$PUT_OBJECT_BIN" "$BUCKET_NAME" "$REL_PATH" "$FILE"
'
