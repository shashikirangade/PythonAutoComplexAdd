import pandas as pd
import sqlite3

# Define column data types explicitly
dtype_parent = {
    'ROW_ID': str,
    'CREATED': str,
    'CREATED_BY': str,
    'LAST_UPD': str,
    'LAST_UPD_BY': str,
    'MODIFICATION_NUM': str,
    'CONFLICT_ID': str,
    'DB_LAST_UPD': str,
    'EFF_END_DT': str,
    'EFF_START_DT': str,
    'X_NUMBER_OF_TIMES': str,
    'COMP_TYPE_CD': str,
    'CONTEXT_CD': str,
    'DB_LAST_UPD_SRC': str,
    'OBJECT_PRODUCT_LIST': str,
    'OBJECT_RELATIONSHIP': str,
    'OBJECT_UID': str,
    'PROD_LIST': str,
    'PTRN_ID': str,
    'SOURCE_RECORD_ID': str,
    'SOURCE_RULE_TYPE': str,
    'SUBJECT_PRODUCT_LIST': str,
    'SUBJECT_RELATIONSHIP': str,
    'SUBJECT_UID': str,
    'X_ACTION_CODE': str,
    'X_CUSTOM_MSG_ENU': str,
    'X_CUSTOM_MSG_FRA': str,
    'X_CUSTOM_MSG_NLD': str,
    'X_OPERATOR': str,
    'X_ORDER_MODE': str,
    'X_PROMOTION_ID': str,
    'PARENT_PRODID': str,
    'ADJ_GROUP_ID': str,
    'GROUP_ID': str,
    'MANUAL_FLAG': str,
    'MTRX_RULE_NUM': str,
    'OBJECT_EVALUATOR': str,
    'PROD_ID': str,
    'PROD_LN_ID': str,
    'PROD_TMPL_VODNUM': str,
    'REL_PROD_COMP_ID': str,
    'REL_PROD_ID': str,
    'REL_PROD_LN_ID': str,
    'REL_PROD_TMPL_VNUM': str,
    'SUBJECT_EVALUATOR': str,
    'X_BGC_QUANTITY_CHECK_FLG': str,
    'X_PROD_CLASS_ID': str,
    'X_REL_PROD_CLASS_ID': str,
    'GROUP_EC': str,
    'X_DECI_PXS_PRODUCT': str,
    'X_DECI_PXS_PRODUCT_ACTION': str,
    'X_DEC_PRD_LN_ID': str,
    'X_CUST_AGREE_ID': str,
    'X_SKIP_RULES_ONBOARD_RENE': str
}

dtype_child = {
    'ROW_ID': str,
    'CREATED': str,
    'CREATED_BY': str,
    'LAST_UPD': str,
    'LAST_UPD_BY': str,
    'MODIFICATION_NUM': str,
    'CONFLICT_ID': str,
    'DB_LAST_UPD': str,
    'CONDITION': str,
    'COUNT': str,
    'DATATYPE': str,
    'DB_LAST_UPD_SRC': str,
    'GROUP_ID': str,
    'OPERATOR': str,
    'ROW_NUM': str,
    'VALUE': str,
    'SOURCE_RECORD_ID': str,
    'COND_PRODUCT_PARENT': str,
    'CONSTRAINT_GROUPTYPE': str,
    'CONSTRAINT_PARENT': str,
    'IS_OBJ_EVALUATOR': str,
    'IS_SUBJ_EVALUATOR': str
}

# Load CSV files into DataFrames with defined data types
parent_df = pd.read_csv('ParentMatrixData.csv', dtype=dtype_parent)
child_df = pd.read_csv('ChildData.csv', dtype=dtype_child)

# Print DataFrames to verify contents
print("Parent DataFrame:")
print(parent_df)

print("\nChild DataFrame:")
print(child_df)

# Ensure the data types match the table schema
# Convert date columns to appropriate format
date_columns_parent = ['CREATED', 'LAST_UPD', 'DB_LAST_UPD', 'EFF_END_DT', 'EFF_START_DT']
date_columns_child = ['CREATED', 'LAST_UPD', 'DB_LAST_UPD']

for col in date_columns_parent:
    if col in parent_df.columns:
        parent_df[col] = pd.to_datetime(parent_df[col], errors='coerce', dayfirst=True)

for col in date_columns_child:
    if col in child_df.columns:
        child_df[col] = pd.to_datetime(child_df[col], errors='coerce', dayfirst=True)

# Remove duplicates based on primary key columns
parent_df = parent_df.drop_duplicates(subset=['ROW_ID'])
child_df = child_df.drop_duplicates(subset=['ROW_ID'])

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('mydatabase.db')

# Create cursor object
cur = conn.cursor()

# Create tables
cur.execute('''
CREATE TABLE IF NOT EXISTS PARENT_COMP_MATRIX (
    ROW_ID TEXT PRIMARY KEY,
    CREATED DATE,
    CREATED_BY TEXT,
    LAST_UPD DATE,
    LAST_UPD_BY TEXT,
    MODIFICATION_NUM TEXT,
    CONFLICT_ID TEXT,
    DB_LAST_UPD DATE,
    EFF_END_DT DATE,
    EFF_START_DT DATE,
    X_NUMBER_OF_TIMES TEXT,
    COMP_TYPE_CD TEXT,
    CONTEXT_CD TEXT,
    DB_LAST_UPD_SRC TEXT,
    OBJECT_PRODUCT_LIST TEXT,
    OBJECT_RELATIONSHIP TEXT,
    OBJECT_UID TEXT,
    PROD_LIST TEXT,
    PTRN_ID TEXT,
    SOURCE_RECORD_ID TEXT,
    SOURCE_RULE_TYPE TEXT,
    SUBJECT_PRODUCT_LIST TEXT,
    SUBJECT_RELATIONSHIP TEXT,
    SUBJECT_UID TEXT,
    X_ACTION_CODE TEXT,
    X_CUSTOM_MSG_ENU TEXT,
    X_CUSTOM_MSG_FRA TEXT,
    X_CUSTOM_MSG_NLD TEXT,
    X_OPERATOR TEXT,
    X_ORDER_MODE TEXT,
    X_PROMOTION_ID TEXT,
    PARENT_PRODID TEXT,
    ADJ_GROUP_ID TEXT,
    GROUP_ID TEXT,
    MANUAL_FLAG TEXT,
    MTRX_RULE_NUM TEXT,
    OBJECT_EVALUATOR TEXT,
    PROD_ID TEXT,
    PROD_LN_ID TEXT,
    PROD_TMPL_VODNUM TEXT,
    REL_PROD_COMP_ID TEXT,
    REL_PROD_ID TEXT,
    REL_PROD_LN_ID TEXT,
    REL_PROD_TMPL_VNUM TEXT,
    SUBJECT_EVALUATOR TEXT,
    X_BGC_QUANTITY_CHECK_FLG TEXT,
    X_PROD_CLASS_ID TEXT,
    X_REL_PROD_CLASS_ID TEXT,
    GROUP_EC TEXT,
    X_DECI_PXS_PRODUCT TEXT,
    X_DECI_PXS_PRODUCT_ACTION TEXT,
    X_DEC_PRD_LN_ID TEXT,
    X_CUST_AGREE_ID TEXT,
    X_SKIP_RULES_ONBOARD_RENE TEXT
);
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS CHILD_RULES (
    ROW_ID TEXT PRIMARY KEY,
    CREATED DATE,
    CREATED_BY TEXT,
    LAST_UPD DATE,
    LAST_UPD_BY TEXT,
    MODIFICATION_NUM TEXT,
    CONFLICT_ID TEXT,
    DB_LAST_UPD DATE,
    CONDITION TEXT,
    COUNT TEXT,
    DATATYPE TEXT,
    DB_LAST_UPD_SRC TEXT,
    GROUP_ID TEXT,
    OPERATOR TEXT,
    ROW_NUM TEXT,
    VALUE TEXT,
    SOURCE_RECORD_ID TEXT,
    COND_PRODUCT_PARENT TEXT,
    CONSTRAINT_GROUPTYPE TEXT,
    CONSTRAINT_PARENT TEXT,
    IS_OBJ_EVALUATOR TEXT,
    IS_SUBJ_EVALUATOR TEXT
);
''')

# Function to insert data with error handling
def insert_data(df, table_name):
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data inserted into {table_name} successfully.")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")

# Insert data into tables
insert_data(parent_df, 'PARENT_COMP_MATRIX')
insert_data(child_df, 'CHILD_RULES')

# Commit changes and close connection
conn.commit()
conn.close()