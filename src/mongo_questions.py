import pandas as pd
import re
from db.mongo_db_setup import *

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

db_name = 'yelp'
conn_string = 'mongodb://yelp:yelp@localhost:27017/{0}'.format(db_name)
client, db = mongo_connect(conn_string, db_name)

# question 1 is covered during data ingestion
# question 2a: top 10 most popular restaurants in Toronto (rating > 4.4, ordered by review count)
try:
    collection_name = 'business'
    collection = db[collection_name]
    print("\n------ Question 2 (option a), top 10 most popular restaurants in Toronto:")
    print("-- Querying collection {0}...".format(collection_name))
    pat = re.compile(r'Restaurants', re.I)
    results = collection.find(
        {"city": "Toronto", "stars": {"$gt": 4.4}, "categories": {"$regex": "/Restaurants/"}},
        {"name": 1, "stars": 1, "review_count": 1, "_id": 0}
    ).sort([("review_count", -1), ("stars", -1)]).limit(10)
    results = pd.DataFrame(results)
    print(results)

except pymongo.errors.OperationFailure as err:
    # do whatever you need
    print("{0} Error when querying database:\n{1}".format('-' * 15, err))

# question 2b: top 10 most popular restaurants in Toronto (ordered by rating and review count)
try:
    collection_name = 'business'
    collection = db[collection_name]
    print("\n------ Question 2 (option b), top 10 most popular restaurants in Toronto:")
    print("-- Querying collection {0}...".format(collection_name))

    results = collection.find(
        {"city": "Toronto", "categories": pat},
        {"name": 1, "stars": 1, "review_count": 1, "_id": 0}
    ).sort([("stars", -1), ("review_count", -1)]).limit(10)
    results = pd.DataFrame(results)
    print(results)

except pymongo.errors.OperationFailure as err:
    # do whatever you need
    print("{0} Error when querying database:\n{1}".format('-' * 15, err))


# Question 3: How many Canadian residents reviewed the business “Mon Ami Gabi” in last 1 year?
try:
    join_key = 'business_id'
    print("\n------ Question 3, number of Canadian residents who reviewed Mon Ami Gabi in the last year:")
    ca_prov_list = ['NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU']
    ca_users = db['business'].aggregate([
        {"$project": {"_id": 0,
                      "b": "$$ROOT",
                      "can_business":
                          {"$cond": [
                              {"$in": ["state", ca_prov_list]}, 1, 0
                          ]}}},
        {"$lookup": {
            "from": "review",
            "localField": "b.business_id",
            "foreignField": "business_id",
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": False}},
        {"$group": {"_id": "$r.user_id", "ca_reviews": {"$sum": "$can_business"}, "total_reviews": {"$sum": 1}}},
        {"$project": {
            "_id": 0, "user_id": "$_id",
            "ca_reviews": "$ca_reviews", "total_reviews": "$total_reviews",
            "ca_ratio": {"$divide": ["$ca_reviews", "$total_reviews"]},
        }},
        {"$match": {"ca_ratio": {"$gt": 0.6}}},
        {"$project": {"ca_users": {"$sum": 1}}}
#         {"$match": {"b.name": "Mon Ami Gabi"}}
    ], allowDiskUse=True)
    # ca_reviews = db['review'].find({"business_id": {"$in": ca_businesses["business_id"].to_list()}})\
    # ca_reviews = db['review'].count_documents({"business_id": {"$in": ca_bus_idx_list}})
#    mon_ami_gabi = db['business'].find_one({"name": "Mon Ami Gabi"})
#    mag_review_users = db['review'].find({"business_id": mon_ami_gabi["business_id"]}).distinct("user_id")
    results = ca_users
    results = pd.DataFrame(results)
    print(results)

except pymongo.errors.OperationFailure as err:
    # do whatever you need
    print("{0} Error when querying database:\n{1}".format('-' * 15, err))


"""
# Question 3: How many Canadian residents reviewed the business “Mon Ami Gabi” in last 1 year?
try:
    collection_name1 = 'business'
    collection_name2 = 'review'
    join_key = 'business_id'
    collection1 = db[collection_name1]
    print("\n------ Question 3, number of Canadian residents who reviewed Mon Ami Gabi in the last year:")
    print("-- Querying collections {0} and {1}...".format(collection_name1, collection_name2))
#    pat = re.compile(r'Mallo', re.I)
    results = collection1.aggregate([
        {"$project": {"_id": 0, "b": "$$ROOT"}},
        {"$lookup":
            {
                "from": collection_name2,
                "localField": "b.{0}".format(join_key),
                "foreignField": join_key,
                "as": "r"
            }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": False}},
        {"$match": {"b.name": "Mon Ami Gabi"}},
        {"$count": "selected_reviews"}
    ])
    results = pd.DataFrame(results)
    print(results)

except pymongo.errors.OperationFailure as err:
    # do whatever you need
    print("{0} Error when querying database:\n{1}".format('-' * 15, err))
"""


client.close()
