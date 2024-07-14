import pandas as pd
import operator
from collections import OrderedDict
import json
import os
import time
import re

# Define file paths
parent_file_path = 'parent.json'
child_file_path = 'child.json'

# Check if files exist
if not os.path.exists(parent_file_path):
    print(f"Error: {parent_file_path} does not exist.")
    exit(1)

if not os.path.exists(child_file_path):
    print(f"Error: {child_file_path} does not exist.")
    exit(1)

# Read JSON files
with open(parent_file_path, 'r') as f:
    parent_data = json.load(f)

with open(child_file_path, 'r') as f:
    child_data = json.load(f)

# Convert JSON data to DataFrame
parent_df = pd.DataFrame(parent_data)
child_df = pd.DataFrame(child_data)

# Custom LRU Cache with hit count
class LRUCache:
    def __init__(self, max_size=100):
        self.cache = OrderedDict()
        self.hits = {}
        self.max_size = max_size

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            self.hits[key] += 1
            return self.cache[key]
        return None

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.max_size:
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
                del self.hits[oldest_key]
            self.cache[key] = value
            self.hits[key] = 1

    def cache_hits(self):
        return self.hits

    def display_cache(self):
        return list(self.cache.keys())

# Initialize caches
child_cache = LRUCache(max_size=100)
parent_cache = LRUCache(max_size=100)

def get_child_records(product_name):
    cache_key = f'child_records:{product_name}'
    cached_data = child_cache.get(cache_key)
    if cached_data:
        return cached_data
    
    data = child_df[child_df['product'] == product_name].to_dict('records')
    child_cache.set(cache_key, data)
    return data

def get_parent_record(source_id):
    cache_key = f'parent_record:{source_id}'
    cached_data = parent_cache.get(cache_key)
    if cached_data:
        return cached_data
    
    data = parent_df[parent_df['source_id'] == source_id].to_dict('records')[0]
    parent_cache.set(cache_key, data)
    return data

operators = {
    '=': operator.eq,
    '<>': operator.ne,
    'LIKE': lambda a, b: b in a
}

def evaluate_condition(condition):
    op_func = operators[condition['operator']]
    return op_func(condition['condition_attribute_name'], condition['value'])

def evaluate_expression(expression, child_records):
    # Replace `OR` and `AND` with `or` and `and`
    expression = expression.replace('OR', 'or').replace('AND', 'and')

    # Create a dictionary to map record IDs to their evaluation results
    eval_dict = {f"row{record['id']}": str(evaluate_condition(record)).capitalize() for record in child_records}

    # Replace each placeholder in the expression with the corresponding boolean result
    for key, value in eval_dict.items():
        expression = re.sub(r'\b{}\b'.format(key), value, expression)

    # Find all `rowXXX` placeholders and replace them with `False` if not already replaced
    all_placeholders = set(re.findall(r'row\d+', expression))
    for placeholder in all_placeholders:
        expression = re.sub(r'\b{}\b'.format(placeholder), 'False', expression)

    # Evaluate the final boolean expression
    try:
        return eval(expression)
    except NameError as e:
        print(f"Error evaluating expression: {expression}")
        raise e

def apply_promo(product_name):
    child_records = get_child_records(product_name)
    source_ids = {record['source_id'] for record in child_records}
    
    applicable_promos = []
    
    for source_id in source_ids:
        parent_record = get_parent_record(source_id)
        child_records_for_parent = [record for record in child_records if record['source_id'] == source_id]
        
        if evaluate_expression(parent_record['evaluation'], child_records_for_parent):
            applicable_promos.append(parent_record['promo'])
    
    return applicable_promos

# Example usage
product_name = 'packimus'

# Capture the start time
start_time = time.time()

# Call the function
promos = apply_promo(product_name)

# Capture the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the results and the execution time
print(f'Applicable promos for {product_name}: {promos}')
print(f'Time taken to execute: {elapsed_time} seconds')
print(f'Child Cache Hits: {child_cache.cache_hits()}')
print(f'Parent Cache Hits: {parent_cache.cache_hits()}')
print(f'Child Cache Contents: {child_cache.display_cache()}')
print(f'Parent Cache Contents: {parent_cache.display_cache()}')