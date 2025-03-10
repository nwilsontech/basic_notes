from elasticsearch import Elasticsearch, helpers
import json
import os

# Configuration
ES_HOST = "http://localhost:9200"
USERNAME = "elastic"
PASSWORD = "your_password"  # 🔒 Replace with your actual password
INDEX_NAME = "other_data"
SCROLL_DURATION = "5m"
BATCH_SIZE = 10000
TOTAL_DOCS = 5000000
OUTPUT_DIR = "./es_scroll_output"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Elasticsearch client with basic auth
es = Elasticsearch(
    ES_HOST,
    basic_auth=(USERNAME, PASSWORD),  # 👈 Auth added here
    verify_certs=False  # Set to True if you're using HTTPS with valid certs
)

# Step 1: Initial search
print("Starting initial search...")
response = es.search(
    index=INDEX_NAME,
    scroll=SCROLL_DURATION,
    size=BATCH_SIZE,
    body={
        "query": {"match_all": {}}
    }
)

scroll_id = response['_scroll_id']
hits = response['hits']['hits']
batch_count = 1
total_retrieved = len(hits)

# Save first batch
with open(f"{OUTPUT_DIR}/batch_{batch_count}.json", "w") as f:
    json.dump(hits, f, indent=2)

print(f"Saved batch {batch_count}, documents: {len(hits)}")

# Step 2: Scroll loop
while hits and total_retrieved < TOTAL_DOCS:
    response = es.scroll(scroll_id=scroll_id, scroll=SCROLL_DURATION)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

    if not hits:
        print("No more results.")
        break

    batch_count += 1
    total_retrieved += len(hits)

    with open(f"{OUTPUT_DIR}/batch_{batch_count}.json", "w") as f:
        json.dump(hits, f, indent=2)

    print(f"Saved batch {batch_count}, documents: {len(hits)} (Total: {total_retrieved})")

print(f"\nScroll completed. {total_retrieved} documents saved in '{OUTPUT_DIR}/'")
