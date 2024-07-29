import pandas as pd
import operator
import sqlite3
import re
import json
from datetime import datetime
from flask import Flask, request, jsonify
import time

app = Flask(__name__)

# Function to get child records matching the conditions from the database
def get_matching_child_records(attributes, profile_attributes, products, conn):
    cursor = conn.cursor()
    matching_records = []

    # Query the child table for each attribute
    for attribute in attributes:
        cursor.execute('SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = "=" AND VALUE = ?', (attribute["Name"], attribute["Value"]))
        records = cursor.fetchall()
        matching_records.extend(records)

    # Query the child table for each profile attribute
    for key, value in profile_attributes.items():
        cursor.execute('SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = "=" AND VALUE = ?', (key, value))
        records = cursor.fetchall()
        matching_records.extend(records)

    # Query the child table for each product
    for product in products:
        cursor.execute('SELECT * FROM CHILD_RULES WHERE CONDITION = ? AND OPERATOR = "=" AND VALUE = ?', ("Product", product["ProductId"]))
        records = cursor.fetchall()
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
    cursor.execute(query, source_record_ids)
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
def evaluate_condition(condition, attributes, profile_attributes, products):
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
    elif condition[10].lower() == 'product':
        for product in products:
            if 'Product' == condition[8]:
                attribute_value = product["ProductId"]
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
    # Ensure balanced parentheses
    open_parens = result.count('(')
    close_parens = result.count(')')
    if open_parens > close_parens:
        result.extend([')'] * (open_parens - close_parens))
    elif close_parens > open_parens:
        result = ['('] * (close_parens - open_parens) + result
    return ' '.join(result)

# Function to evaluate the final expression
def evaluate_expression(expression, child_records, attributes, profile_attributes, products):
    # Create a dictionary to map record IDs to their evaluation results
    eval_dict = {f"{record[14]}": str(evaluate_condition(record, attributes, profile_attributes, products)).capitalize() for record in child_records}

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
def apply_promo(attributes, profile_attributes, products):
    start_time = time.time()  # Start timing
    conn = sqlite3.connect('mydatabase.db')
    
    # Get child records matching the conditions
    matching_child_records = get_matching_child_records(attributes, profile_attributes, products, conn)
    
    if not matching_child_records:
        conn.close()
        return []

    # Get unique source record IDs from matching child records
    source_record_ids = list(set(record[16] for record in matching_child_records))

    # Get parent records using the source record IDs
    parent_records = get_parent_records(source_record_ids, conn)
    
    applicable_promos = []
    
    for parent_record in parent_records:
        source_record_id = parent_record[19]
        subject_evaluator = parent_record[44]  # assuming this is the correct index for the evaluation field

        # Get all child records associated with this parent record's source ID
        child_records = get_child_records_by_source_id(source_record_id, conn)
        
        # Evaluate expression for each product
        for product in products:
            if evaluate_expression(subject_evaluator, child_records, attributes, profile_attributes, [product]):
                # Check if object_product_list column in parent record matches the ProductId or ParentProdId
                if product["ParentProdId"]:
                    if parent_record[31] == product["ParentProdId"]:
                        applicable_promos.append({
                            "sourcerecordid": source_record_id,
                            "rootproductid": product["ProductId"],
                            "rowid": product["rowid"]
                        })
                else:
                    if parent_record[31] == product["ProductId"]:
                        applicable_promos.append({
                            "sourcerecordid": source_record_id,
                            "rootproductid": product["ProductId"],
                            "rowid": product["rowid"]
                        })

    conn.close()
    end_time = time.time()  # End timing
    print(f'Time taken to execute apply_promo: {end_time - start_time} seconds')
    return applicable_promos

@app.route('/apply_promo', methods=['POST'])
def apply_promo_api():
    input_json = request.json
    if not input_json:
        return jsonify({"error": "Invalid input"}), 400

    attributes = input_json.get('attributes', [])
    profile_attributes = input_json.get('profileattributes', {})
    products = input_json.get('products', [])

    try:
        promos = apply_promo(attributes, profile_attributes, products)
        return jsonify({"promos": promos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)