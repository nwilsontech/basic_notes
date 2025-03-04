def flatten_json_with_lists(nested_json, prefix=''):
    """
    Flatten a nested JSON object, but for lists of objects,
    return multiple flattened objects (one for each list item).

    Returns a list of flattened dictionaries.
    """
    # If we're processing a list of objects at the top level
    if isinstance(nested_json, list):
        result = []
        for item in nested_json:
            result.extend(flatten_json_with_lists(item, prefix))
        return result

    # Start with a single flattened object
    result = [{}]

    for key, value in nested_json.items():
        new_key = f"{prefix}{key}" if prefix else key

        if isinstance(value, dict):
            # For nested dictionaries, flatten and update all current results
            nested_flat = flatten_json_with_lists(value, f"{new_key}.")
            if len(nested_flat) == 1:
                for r in result:
                    r.update(nested_flat[0])
            else:
                # This shouldn't happen in normal cases but handle it just in case
                new_result = []
                for r in result:
                    for nf in nested_flat:
                        new_r = r.copy()
                        new_r.update(nf)
                        new_result.append(new_r)
                result = new_result

        elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
            # For lists of objects, create multiple flattened objects
            if not value:  # Empty list
                for r in result:
                    r[new_key] = []
            else:
                # Flatten each object in the list
                list_of_flattened = []
                for item in value:
                    flattened_items = flatten_json_with_lists(item, f"{new_key}.")
                    list_of_flattened.extend(flattened_items)

                # Create a new result for each item in the list
                new_result = []
                for r in result:
                    for flattened_item in list_of_flattened:
                        new_r = r.copy()
                        new_r.update(flattened_item)
                        new_result.append(new_r)
                result = new_result

        elif isinstance(value, list):
            # For lists of primitives, keep the array notation
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    nested_flat = flatten_json_with_lists(item, f"{new_key}[{i}].")
                    for r in result:
                        r.update(nested_flat[0])
                else:
                    for r in result:
                        r[f"{new_key}[{i}]"] = item
        else:
            # For primitive values, add to all current results
            for r in result:
                r[new_key] = value

    return result

# Example usage
nested = {
    "id": True,
    "trace": [
        {"t": "a", "c": "b"},
        {"t": "c", "c": "d"}
    ]
}

flattened_list = flatten_json_with_lists(nested)
for item in flattened_list:
    print(item)
