import pandas as pd
import operator
import sqlite3
import re
import time
import json
from datetime import datetime

# Function to get parent records from the database
def get_parent_records(product_id, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM PARENT_COMP_MATRIX WHERE PARENT_PRODID = ?', (product_id,))
    data = cursor.fetchall()
    return data

# Function to get child records from the database
def get_child_records(source_record_id, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM CHILD_RULES WHERE SOURCE_RECORD_ID = ?', (source_record_id,))
    data = cursor.fetchall()
    return data

# Operators mapping
operators = {
    '=': operator.eq,
    '<>': operator.ne,
    'LIKE': lambda a, b: b in a,
    '!=': operator.ne  # Add the '!=' operator here
}

# Function to evaluate a condition
def evaluate_condition(condition, attributes, profile_attributes):
    attribute_value = None
    if condition[10].lower() == 'attribute':
        attribute_value = attributes.get(condition[8])
    elif condition[10].lower() == 'profile attribute':
        attribute_value = profile_attributes.get(condition[8])
    
    if attribute_value is None:
        return False
    
    op_func = operators.get(condition[13])
    if op_func is None:
        print(f"Unsupported operator: {condition[13]}")
        return False

    try:
        return op_func(attribute_value, condition[15])
    except Exception as e:
        print(f"Error evaluating condition: {condition}, error: {e}")
        return False

# Function to build a valid boolean expression
def build_expression(expression, eval_dict):
    if expression is None:
        return "False"
    tokens = re.findall(r'R-\w+|OR|AND|\(|\)', expression)
    result = []
    for token in tokens:
        if token in eval_dict:
            result.append(eval_dict[token])
        elif token == 'OR':
            result.append('or')
        elif token == 'AND':
            result.append('and')
        else:
            result.append(token)
    return ' '.join(result)

# Function to evaluate the final expression
def evaluate_expression(expression, child_records, attributes, profile_attributes):
    # Create a dictionary to map record IDs to their evaluation results
    eval_dict = {f"{record[14]}": str(evaluate_condition(record, attributes, profile_attributes)).capitalize() for record in child_records}

    # Build a valid boolean expression
    expression = build_expression(expression, eval_dict)

    # Evaluate the final boolean expression
    try:
        return eval(expression)
    except SyntaxError as e:
        print(f"Syntax error in expression: {expression}")
        raise e
    except NameError as e:
        print(f"Name error in expression: {expression}")
        raise e

# Function to apply promo
def apply_promo(input_json):
    product_id = input_json['product_id']
    attributes = input_json['attributes']
    profile_attributes = input_json['profileattributes']
    
    conn = sqlite3.connect('mydatabase.db')
    parent_records = get_parent_records(product_id, conn)
    
    applicable_promos = []
    
    for parent_record in parent_records:
        source_record_id = parent_record[19]
        subject_evaluator = parent_record[44]  # assuming this is the correct index for the evaluation field
        child_records = get_child_records(source_record_id, conn)
        
        if evaluate_expression(subject_evaluator, child_records, attributes, profile_attributes):
            applicable_promos.append(parent_record[19])
    
    conn.close()
    return applicable_promos

# Example usage
input_json = {
    "product_id": "KDKWA",
    "attributes": {
        "Prod Prom Name": "Ambassador Internet Fiber + Tel",
        "BGC Promotion Parent Action Code": "Add"
    },
    "profileattributes": {
        "BGC_MOVE_MIGRATE_STATUS": "Add",
        "BGC_ORIGINAL_MOVE_MIGRATE_STATUS": "Add",
        "BGC_Inet": "New",
        "BGC_BF_ZONE": "Fiber - Fiber",
        "BGC_BF_ACCESS_TYPE": "Fiber - Fiber",
        "BGC_TV": "New"
    }
}

# Capture the start time
start_time = time.time()

# Call the function
promos = apply_promo(input_json)

# Capture the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the results and the execution time
print(f'Applicable promos for product {input_json["product_id"]}: {promos}')
print(f'Time taken to execute: {elapsed_time} seconds')