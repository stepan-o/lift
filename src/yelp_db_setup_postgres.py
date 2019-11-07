import os
from glob import glob
from db.postgres_db_setup import *

conn_params = {'host': 'localhost',
               'port': '5432',
               'database': 'yelp',
               'user': 'yelp',
               'password': 'yelp'}

# read all SQL scripts from sql/DDL folder
ddl_path = os.path.join('sql', 'DDL')
ddl_file_list = glob(os.path.join(ddl_path, '*.sql'))
ddl_query_list = list()
for ddl_file in ddl_file_list:
    with open(ddl_file, 'r') as f:
        ddl_query_list.append(f.read())

# read all SQL scripts from sql/DDL folder
dml_path = os.path.join('sql', 'DML')
dml_file_list = glob(os.path.join(dml_path, '*.sql'))
dml_query_list = list()
for dml_file in dml_file_list:
    with open(dml_file, 'r') as f:
        dml_query_list.append(f.read())

# set file paths, generate ingestion list
data_path = os.path.join('data', 'yelp_dataset')
business_path = os.path.join(data_path, 'business.json')
user_path = os.path.join(data_path, 'user.json')
review_path = os.path.join(data_path, 'review.json')
ingest_business = {'file_path': business_path, 'target_table': 'business'}
ingest_user = {'file_path': user_path, 'target_table': 'yelp_user'}
ingest_review = {'file_path': review_path, 'target_table': 'review'}
ing_list = [ingest_business, ingest_user, ingest_review]

# set up database
db_setup(conn_params, ddl_queries=ddl_query_list, ingest_list=ing_list)
