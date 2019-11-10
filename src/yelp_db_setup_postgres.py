import os
from glob import glob
from db.postgres_db_setup import *
from db.source_file_list import source_file_list

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

ing_list = source_file_list()

# set up database
db_setup(conn_params, ddl_queries=ddl_query_list, ingest_list=ing_list)
