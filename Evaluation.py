import pandas as pd
import operator
import sqlite3
import re
import time
import json
from datetime import datetime

# Function to get child records matching the conditions from the database
def get_matching_child_records(attributes, profile_attributes, conn):
    cursor = conn.cursor()
    matching_records = []

    # Query the child table for each attribute
    for attribute in attributes:
        cursor.execute('SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = "=" AND VALUE = ?', (attribute["Name"], attribute["Value"]))
        records = cursor.fetchall()
        print(f'Query for attribute {attribute["Name"]}={attribute["Value"]} returned {len(records)} records.')
        matching_records.extend(records)

    # Query the child table for each profile attribute
    for key, value in profile_attributes.items():
        cursor.execute('SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = "=" AND VALUE = ?', (key, value))
        records = cursor.fetchall()
        print(f'Query for profile attribute {key}={value} returned {len(records)} records.')
        matching_records.extend(records)

    return matching_records

# Function to get all child records associated with a specific source record ID
def get_child_records_by_source_id(source_record_id, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM CHILD_RULES WHERE SOURCE_RECORD_ID = ?', (source_record_id,))
    data = cursor.fetchall()
    return data

# Function to get parent records from the database
def get_parent_records(source_record_ids, conn):
    cursor = conn.cursor()
    query = 'SELECT * FROM PARENT_COMP_MATRIX WHERE SOURCE_RECORD_ID IN ({})'.format(','.join('?' for _ in source_record_ids))
    print(f'Executing query: {query} with source_record_ids: {source_record_ids}')
    cursor.execute(query, source_record_ids)
    data = cursor.fetchall()
    print(f'Parent records query returned {len(data)} records.')
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
        for attribute in attributes:
            if attribute["Name"] == condition[8]:
                attribute_value = attribute["Value"]
                if operators.get(condition[13])(attribute_value, condition[15]):
                    return True
    elif condition[10].lower() == 'profile attribute':
        attribute_value = profile_attributes.get(condition[8])
        if attribute_value is not None:
            if operators.get(condition[13])(attribute_value, condition[15]):
                return True
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
def apply_promo(attributes, profile_attributes):
    conn = sqlite3.connect('mydatabase.db')
    
    # Get child records matching the conditions
    matching_child_records = get_matching_child_records(attributes, profile_attributes, conn)
    
    if not matching_child_records:
        print('No matching child records found.')
        return []

    # Get unique source record IDs from matching child records
    source_record_ids = list(set(record[16] for record in matching_child_records))
    print(f'Unique source record IDs: {source_record_ids}')

    # Get parent records using the source record IDs
    parent_records = get_parent_records(source_record_ids, conn)
    
    applicable_promos = []
    
    for parent_record in parent_records:
        source_record_id = parent_record[19]
        subject_evaluator = parent_record[44]  # assuming this is the correct index for the evaluation field

        # Get all child records associated with this parent record's source ID
        child_records = get_child_records_by_source_id(source_record_id, conn)
        
        if evaluate_expression(subject_evaluator, child_records, attributes, profile_attributes):
            applicable_promos.append(source_record_id)
    
    conn.close()
    return applicable_promos

# Main script execution
# Hardcoded input for debugging
input_json = {
    "attributes": [
        {"Name": "Prod Prom Name", "Value": "Flex Fiber Ambassador", "Type": "Attribute"},
        {"Name": "Prod Prom Name", "Value": "Home fiber", "Type": "Attribute"}
    ],
    "profileattributes": {
        "BGC_BF_ACCESS_TYPE": "Both",
        "BGC_MOVE_MIGRATE_STATUS": "Add",
        "BGC_ORIGINAL_MOVE_MIGRATE_STATUS": "Add",
        "BGC_BF_ZONE": "Copper - Copper"
    }
}

# Extracting attributes and profile attributes
attributes = input_json['attributes']
profile_attributes = input_json['profileattributes']

# Call the function
promos = apply_promo(attributes, profile_attributes)

# Print the results
print(f'Applicable promos: {promos}')