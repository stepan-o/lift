from db.mongo_db_setup import *
from db.source_file_list import source_file_list


# set up database
db_name = 'yelp'
conn_string = 'mongodb://yelp:yelp@localhost:27017/{0}'.format(db_name)
client, db = mongo_connect(conn_string, db_name)
print("------ Ingesting data")
ing_list = source_file_list()
for data_source in ing_list:
    try:
        print("\n------ Inserting data into collection '{0}' from file '{1}'"
              .format(data_source['target_table'], data_source['file_path']))
        json_to_mongo(db, data_source['target_table'], data_source['file_path'])

    except pymongo.errors.OperationFailure as err:
        # do whatever you need
        print("{0} Error when ingesting data into {1}:\n{2}".format('-' * 15, data_source['target_table'], err))

client.close()

