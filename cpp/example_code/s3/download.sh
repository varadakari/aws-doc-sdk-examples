#!/bin/bash
# download_all_objects.sh

set -euo pipefail

if [ $# -ne 5 ]; then
    echo "Usage: $0 <bucket-name> <object-list-file> <download-binary> <destination-root-dir> <concurrency>"
    exit 1
fi

BUCKET_NAME="$1"
OBJECT_LIST_FILE="$2"
DOWNLOAD_BIN="$3"
DEST_DIR="$4"
CONCURRENCY="$5"

# Function to download one object
download_object() {
    local key="$1"
    local dest_path="${DEST_DIR}/${key}"
    local dest_dir_path
    dest_dir_path=$(dirname "$dest_path")

    mkdir -p "$dest_dir_path"

    echo "Downloading s3://${BUCKET_NAME}/${key} to $dest_path"
    "$DOWNLOAD_BIN" "$BUCKET_NAME" "$key" "$dest_path"
}

export -f download_object
export BUCKET_NAME
export DOWNLOAD_BIN
export DEST_DIR

# Use GNU Parallel or fallback to a semaphore-based solution
if command -v parallel > /dev/null 2>&1; then
    cat "$OBJECT_LIST_FILE" | parallel -j "$CONCURRENCY" download_object {}
else
    echo "GNU parallel not found. Falling back to basic parallelism with background jobs."
    
    # Limit background jobs with a semaphore array
    SEMAPHORE=0
    while IFS= read -r key || [ -n "$key" ]; do
        key=$(echo "$key" | xargs)
        if [[ -z "$key" ]]; then
            continue
        fi

        download_object "$key" &

        ((SEMAPHORE++))
        if (( SEMAPHORE >= CONCURRENCY )); then
            wait -n
            ((SEMAPHORE--))
        fi
    done < "$OBJECT_LIST_FILE"

    wait  # Wait for remaining background jobs
fi

