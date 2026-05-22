#!/bin/bash

BASE_URL="http://localhost:8080/bitcask"
TIMESTAMP=$(date +%s)

usage() {
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view --key=SOME_KEY"
    echo "  ./bitcask_client.sh --perf --clients=100"
    exit 1
}

view_all() {
    echo "Fetching all keys from BitCask..."
    OUTPUT_FILE="${TIMESTAMP}.csv"
    echo "key,value" > "$OUTPUT_FILE"

    curl -s "${BASE_URL}/all" | \
        python3 -c "
import sys, json
data = json.load(sys.stdin)
for k, v in data.items():
    # escape commas in value
    v_clean = str(v).replace('\"', '\"\"')
    print(f'{k},\"{v_clean}\"')
" >> "$OUTPUT_FILE"

    echo "Saved to $OUTPUT_FILE"
    cat "$OUTPUT_FILE"
}

view_key() {
    KEY=$1
    echo "Fetching key: $KEY"
    curl -s "${BASE_URL}/get?key=${KEY}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print('Key:   ' + data.get('key',''))
print('Value: ' + data.get('value',''))
"
}

perf_test() {
    CLIENTS=$1
    echo "Starting performance test with $CLIENTS threads..."

    run_client() {
        THREAD_NUM=$1
        TIMESTAMP=$(date +%s)
        OUTPUT_FILE="${TIMESTAMP}_thread_${THREAD_NUM}.csv"
        echo "key,value" > "$OUTPUT_FILE"

        curl -s "${BASE_URL}/all" | \
            python3 -c "
import sys, json
data = json.load(sys.stdin)
for k, v in data.items():
    v_clean = str(v).replace('\"', '\"\"')
    print(f'{k},\"{v_clean}\"')
" >> "$OUTPUT_FILE"

        echo "Thread $THREAD_NUM done -> $OUTPUT_FILE"
    }

    export -f run_client

    # Launch all threads in parallel
    for i in $(seq 1 $CLIENTS); do
        run_client $i &
    done

    wait
    echo "All $CLIENTS threads completed."
}

# ── Argument parsing ──────────────────────────────────────────────────────────
if [[ $# -eq 0 ]]; then
    usage
fi

COMMAND=""
KEY=""
CLIENTS=10

for arg in "$@"; do
    case $arg in
        --view-all)   COMMAND="view-all" ;;
        --view)       COMMAND="view" ;;
        --perf)       COMMAND="perf" ;;
        --key=*)      KEY="${arg#*=}" ;;
        --clients=*)  CLIENTS="${arg#*=}" ;;
        *) echo "Unknown argument: $arg"; usage ;;
    esac
done

case $COMMAND in
    view-all) view_all ;;
    view)
        if [[ -z "$KEY" ]]; then
            echo "Error: --key=SOME_KEY is required for --view"
            usage
        fi
        view_key "$KEY"
        ;;
    perf) perf_test "$CLIENTS" ;;
    *) usage ;;
esac
