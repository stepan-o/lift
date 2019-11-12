import pandas as pd
import re
from db.mongo_db_setup import *

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_colwidth', 300)
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
        {"city": "Toronto", "stars": {"$gt": 4.4}, "categories": pat},
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
    print("\n------ Question 3, number of Canadian residents who reviewed Mon Ami Gabi in the last year:")
    ca_prov_list = ['NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU']
    ca_users_pipeline = [
        {"$project": {
            "_id": 0,
            "b_name": "$name",
            "b_state": "$state",
            "b_business_id": "$business_id",
            "b_ca_business": {
                "$cond": [
                    {
                        "$in": ["$state", ca_prov_list]
                    },
                    1, 0
                ]
            }
        }},
        {"$lookup": {
            "from": "review",
            "localField": "b_business_id",
            "foreignField": "business_id",
            "as": "r"
        }},
        {"$unwind": {
            "path": "$r",
            "preserveNullAndEmptyArrays": False
        }},
        {"$project": {
            "name": "$b_name",
            "ca_business": "$b_ca_business",
            "user_id": "$r.user_id"
        }},
        {"$group": {
            "_id": "$user_id",
            "ca_reviews": {
                "$sum": "$ca_business"
            },
            "total_reviews": {
                "$sum": 1
            }
        }},
        {"$project": {
            "_id": 0,
            "user_id": "$_id",
            "ca_ratio": {
                "$divide": [
                    {
                        "$convert": {
                            "input": "$ca_reviews",
                            "to": "decimal"
                        }
                    },
                    "$total_reviews"
                ]
            }
        }},
        {"$match": {
            "ca_ratio": {
                "$gt": 0.6
            }
        }}
    ]
#    explain = db.command('aggregate', 'business', pipeline=ca_users_pipeline, explain=True)
#    print(explain['stages'])
    t = time()
    ca_users = db['business'].aggregate(ca_users_pipeline, allowDiskUse=True)
    ca_users = pd.DataFrame(ca_users)
    ca_users = ca_users['user_id'].to_list()
    print("CA users: {0:,} users total".format(len(ca_users)))

    mon_ami_gabi = db['business'].find_one({"name": "Mon Ami Gabi"})
    pat = re.compile(r'2018', re.I)
    mag_review_users = db['review'].find({
        "business_id": mon_ami_gabi["business_id"],
        "date": pat
    }).distinct("user_id")
    print("Users who reviewed Mon Ami Gabi in 2018: {0:,} users total".format(len(mag_review_users)))

    intersection = [user for user in mag_review_users if user in ca_users]
    elapsed = time() - t
    print("-- Query completed, took {0:,.2f} seconds ({1:,.2f} minutes)."
          .format(elapsed, elapsed / 60))
    print("{0:,} users from Canada have reviewed Mon Ami Gabi in the past year".format(len(intersection)))

except pymongo.errors.OperationFailure as err:
    # do whatever you need
    print("{0} Error when querying database:\n{1}".format('-' * 15, err))


client.close()
