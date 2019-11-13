# script to setup MongoDB database for Yelp dataset, ingest data, and create indices

from src.db.mongo_db_setup import *
from src.db.source_file_list import *
import os


# set up database
db_name = 'yelp'
conn_string = 'mongodb://yelp:yelp@localhost:27017/{0}'.format(db_name)
client, db = mongo_connect(conn_string, db_name)
print("------ Ingesting data")
ing_list = get_source_file_list()
# data_path = os.path.join('data', 'yelp_dataset')
# review_path = os.path.join(data_path, 'review.json')
# ingest_review = {'file_path': review_path, 'target_table': 'review'}
# ing_list = [ingest_review]

for data_source in ing_list:
    try:
        print("\n------ Inserting data into collection '{0}' from file '{1}'"
              .format(data_source['target_table'], data_source['file_path']))
        json_to_mongo(db, data_source['target_table'], data_source['file_path'])

    except pymongo.errors.OperationFailure as err:
        print("{0} Error when ingesting data into {1}:\n{2}".format('-' * 15, data_source['target_table'], err))

idx_dict_list = get_idx_dict_list()
print("\n------ Setting up indexes")
for idx_dict in idx_dict_list:
    mongo_index(db, idx_dict)
print("------ All indexes have been created\n")

print("------ Closing connection to database")

client.close()
