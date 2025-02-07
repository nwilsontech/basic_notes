import re

sql_queries = [
    "SELECT column1, column2, column3 FROM table",
    "SELECT col1 as alias1, COUNT(*), MAX(col2) FROM table",
    "SELECT a.field1, b.field2, CONCAT(a.f1, b.f2) as combined FROM table"
]

def extract_columns(sql):
    # First, extract everything between SELECT and FROM
    main_pattern = r'(?i)SELECT\s+((?:(?:(?!\bFROM\b|\bSELECT\b)[^,])+,?\s*)+)\s+FROM'
    # Then split into individual columns
    column_pattern = r'[^,\s][^,]*[^,\s]|[^,\s]'

    match = re.search(main_pattern, sql)
    if match:
        columns_str = match.group(1)
        columns = re.findall(column_pattern, columns_str)
        return [col.strip() for col in columns]
    return []

# Test the function
for sql in sql_queries:
    print("\nSQL:", sql)
    print("Columns:", extract_columns(sql))

# Test cases
sql_queries = [
    "SELECT data_id, name FROM table",
    "SELECT id, my_data_id, name FROM table",
    "SELECT user_data_id, data_id_temp FROM table",
    "SELECT id, name, data_id as identifier FROM table",
    "SELECT COUNT(data_id) FROM table",
    "SELECT name FROM table"
]

pattern = r'(?i)SELECT\s+(?:(?!\bFROM\b).)*\b(data_id)\b(?:(?!\bFROM\b).)*\s+FROM'

for sql in sql_queries:
    match = re.search(pattern, sql)
    print(f"\nSQL: {sql}")
    print(f"Contains 'data_id'?: {bool(match)}")
