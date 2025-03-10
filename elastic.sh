#!/bin/bash

ES_HOST="http://localhost:9200"
INDEX_NAME="other_data"
SCROLL_DURATION="5m"
BATCH_SIZE=10000
TOTAL_DOCS=5000000
TOTAL_BATCHES=$((TOTAL_DOCS / BATCH_SIZE))
OUTPUT_DIR="./es_scroll_output"

mkdir -p "$OUTPUT_DIR"

echo "Starting initial search..."

# Step 1: Initial Search
RESPONSE=$(curl -s -X POST "$ES_HOST/$INDEX_NAME/_search?scroll=$SCROLL_DURATION" \
  -H 'Content-Type: application/json' -d"
{
  \"size\": $BATCH_SIZE,
  \"query\": {
    \"match_all\": {}
  }
}")

SCROLL_ID=$(echo "$RESPONSE" | jq -r '._scroll_id')
echo "$RESPONSE" > "$OUTPUT_DIR/batch_1.json"

echo "Scroll ID obtained. Starting scroll loop..."

# Step 2: Continue scrolling
for ((i=2; i<=TOTAL_BATCHES; i++)); do
  echo "Fetching batch $i..."
  RESPONSE=$(curl -s -X POST "$ES_HOST/_search/scroll" \
    -H 'Content-Type: application/json' -d"
{
  \"scroll\": \"$SCROLL_DURATION\",
  \"scroll_id\": \"$SCROLL_ID\"
}")

  # Check if no more results
  HITS=$(echo "$RESPONSE" | jq '.hits.hits | length')
  if [[ "$HITS" -eq 0 ]]; then
    echo "No more results. Ending scroll."
    break
  fi

  echo "$RESPONSE" > "$OUTPUT_DIR/batch_$i.json"
  SCROLL_ID=$(echo "$RESPONSE" | jq -r '._scroll_id')
done

echo "Scroll completed. All batches saved in '$OUTPUT_DIR/'"
