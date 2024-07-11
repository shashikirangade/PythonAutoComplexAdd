import pandas as pd
import operator
from collections import OrderedDict

# Read Excel files
parent_df = pd.read_excel('parent.xlsx')
child_df = pd.read_excel('child.xlsx')

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
    for record in child_records:
        result = evaluate_condition(record)
        expression = expression.replace(str(record['id']), str(result))
    return eval(expression)

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
promos = apply_promo(product_name)
print(f'Applicable promos for {product_name}: {promos}')
print(f'Child Cache Hits: {child_cache.cache_hits()}')
print(f'Parent Cache Hits: {parent_cache.cache_hits()}')
print(f'Child Cache Contents: {child_cache.display_cache()}')
print(f'Parent Cache Contents: {parent_cache.display_cache()}')