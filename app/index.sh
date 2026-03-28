#!/bin/bash

INPUT_PATH=${1:-/input/data}

echo "================================================================"
echo " index.sh  —  full indexing pipeline"
echo " Input: $INPUT_PATH"
echo "================================================================"

cd "$(dirname "$0")"

echo ""
echo "[ Step 1 / 2 ]  Running create_index.sh ..."
echo "----------------------------------------------------------------"
bash create_index.sh "$INPUT_PATH"

if [ $? -ne 0 ]; then
    echo "[ERROR] create_index.sh failed. Aborting."
    exit 1
fi

echo ""
echo "[ Step 1 / 2 ]  create_index.sh finished successfully."

echo ""
echo "[ Step 2 / 2 ]  Running store_index.sh ..."
echo "----------------------------------------------------------------"
bash store_index.sh

if [ $? -ne 0 ]; then
    echo "[ERROR] store_index.sh failed."
    exit 1
fi

echo ""
echo "[ Step 2 / 2 ]  store_index.sh finished successfully."

echo ""
echo "================================================================"
echo " Indexing pipeline complete!"
echo "================================================================"
