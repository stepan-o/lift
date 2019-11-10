import pymongo
import json
from time import time


def mongo_connect(conn_string, db_name):
    print("------ Connecting to MongoDB database")
    try:
        client = pymongo.MongoClient(conn_string)
        print("Connected, displaying server information:\n{0}\n\n-- database names:\n{1}\n"
              .format(client.server_info(), client.list_database_names()))
        print("Getting database '{0}'".format(db_name))
        db = client[db_name]
        print("Collection names:\n{0}".format(db.list_collection_names()))
        return client, db

    except pymongo.errors.OperationFailure as err:
        # do whatever you need
        print("{0} Error when connecting to database:\n{1}".format('-' * 15, err))


def json_to_mongo(db, collection_name, json_path):
    db[collection_name].drop()
    collection = db[collection_name]
    t = time()
    with open(json_path, 'r') as f_in:
        lines = f_in.readlines()
        row = 0
        for line in lines:
            if row % 50000 == 0:
                elpsd = time() - t
                print("Inserted {0:,.0f} rows, so far took {1:,.2f} seconds ({2:,.2f} minutes)"
                      .format(row, elpsd, elpsd / 60))
            row += 1
            line_json = json.loads(line)
            collection.insert_one(line_json)
        elapsed = time() - t
        print("Data inserted, took {0:,.2f} seconds ({1:,.2f} minutes)"
              .format(elapsed, elapsed / 60))
    return collection
