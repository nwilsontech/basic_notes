def flatten_json(nested_json, parent_key='', sep='.'):
    flattened = {}

    def recursive_flatten(obj, parent_key):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                recursive_flatten(v, new_key)
        elif isinstance(obj, list):
            flattened[parent_key] = [flatten_json(item) for item in obj]  # Convert list items into separate objects
        else:
            flattened[parent_key] = obj

    recursive_flatten(nested_json, parent_key)
    return flattened

# Example Usage:
import json

nested_json = {
    "user": {
        "name": "John",
        "address": {
            "city": "New York",
            "zip": "10001"
        }
    },
    "order": {
        "id": 12345,
        "items": [
            {"name": "Laptop", "price": 1200},
            {"name": "Mouse", "price": 50}
        ]
    }
}

flattened_json = flatten_json(nested_json)
print(json.dumps(flattened_json, indent=2))
