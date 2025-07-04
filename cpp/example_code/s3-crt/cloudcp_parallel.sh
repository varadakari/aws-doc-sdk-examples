#!/bin/bash
# cloudcp_parallel.sh

set -euo pipefail
set -x

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <src> <dst> [--endpoint-url <url>]"
    echo "Example:"
    echo "  $0 ./data s3://bucket/prefix --endpoint-url https://10.10.10.155:9000"
    echo "  $0 s3://bucket/prefix ./restore --endpoint-url https://10.10.10.155:9000"
    exit 1
fi

SRC="$1"
DST="$2"
shift 2 || true

ENDPOINT_OPT=""
ENDPOINT_URL=""
if [[ $# -ge 2 && "$1" == "--endpoint-url" ]]; then
    ENDPOINT_OPT="--endpoint-url $2"
    ENDPOINT_URL="$2"
fi

# Setup paths
CLOUDCP_BIN="./run_cloudcp"
#LD_LIBRARY_PATH="/opt/bryck/aws"
LD_LIBRARY_PATH="/usr/local/lib"
export LD_LIBRARY_PATH

# Default concurrency = number of CPU cores
#CONCURRENCY=$(nproc)
CONCURRENCY=15

# Upload
if [[ "$DST" == s3://* ]]; then
    echo "Running in UPLOAD mode: $SRC → $DST"

    find "$SRC" -type f | parallel -j "$CONCURRENCY" --env LD_LIBRARY_PATH "
        FILE={}
        REL_PATH=\${FILE#"$SRC"/}
        S3_URI=\"$DST/\$REL_PATH\"
        echo \"[upload] \$FILE → \$S3_URI\"
	LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2 \
        \"$CLOUDCP_BIN\" $ENDPOINT_OPT \"\$FILE\" \"\$S3_URI\"
    "

# Download
elif [[ "$SRC" == s3://* ]]; then
    echo "Running in DOWNLOAD mode: $SRC → $DST"
    mkdir -p "$DST"

    BUCKET=$(echo "$SRC" | cut -d/ -f3)
    PREFIX=$(echo "$SRC" | cut -d/ -f4-)

    echo "Fetching object list from S3..."
    #OBJECTS=$(aws s3 ls "s3://$BUCKET/$PREFIX" --recursive $ENDPOINT_OPT | awk '{$1=$2=$3=""; print substr($0,4)}')
    OBJECTS=$(aws s3 ls "s3://$BUCKET/$PREFIX" --recursive $ENDPOINT_OPT | awk '{$1=$2=$3=""; print substr($0,4)}' | grep -v '^$')


    #echo "$OBJECTS" | parallel -j "$CONCURRENCY" --env LD_LIBRARY_PATH "
    #    REL_PATH={}
    #    SRC_URI=\"s3://$BUCKET/$REL_PATH\"
    #    DST_PATH=\"$DST/$REL_PATH\"
    #    mkdir -p \$(dirname \"\$DST_PATH\")
    #    echo \"[download] \$SRC_URI → \$DST_PATH\"
    #    \"$CLOUDCP_BIN\" $ENDPOINT_OPT \"\$SRC_URI\" \"\$DST_PATH\"
    #"
    echo "$OBJECTS" | parallel -j "$CONCURRENCY" --env LD_LIBRARY_PATH "
    REL_PATH={}
    if [[ -z \"\$REL_PATH\" ]]; then
        echo 'Skipping empty REL_PATH'
        exit 0
    fi
    SRC_URI=\"s3://$BUCKET/\$REL_PATH\"
    DST_PATH=\"$DST/\$REL_PATH\"
    mkdir -p \$(dirname \"\$DST_PATH\")
    echo \"[download] \$SRC_URI → \$DST_PATH\"
    LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2 \
    \"$CLOUDCP_BIN\" $ENDPOINT_OPT \"\$SRC_URI\" \"\$DST_PATH\"
    "


else
    echo "❌ Unable to determine upload or download direction."
    exit 1
fi
