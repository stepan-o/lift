import pymongo
import os


def get_source_file_list():
    data_path = os.path.join('data', 'yelp_dataset')
    business_path = os.path.join(data_path, 'business.json')
    user_path = os.path.join(data_path, 'user.json')
    review_path = os.path.join(data_path, 'review.json')
    checkin_path = os.path.join(data_path, 'checkin.json')
    tip_path = os.path.join(data_path, 'tip.json')
    ingest_business = {'file_path': business_path, 'target_table': 'business'}
    ingest_user = {'file_path': user_path, 'target_table': 'yelp_user'}
    ingest_review = {'file_path': review_path, 'target_table': 'review'}
    ingest_checkin = {'file_path': checkin_path, 'target_table': 'checkin'}
    ingest_tip = {'file_path': tip_path, 'target_table': 'tip'}
    ing_list = [ingest_business, ingest_user, ingest_review, ingest_checkin, ingest_tip]
    return ing_list


def get_idx_dict_list():
    idx_dict_list = [
        {
            "cole_name": "business",
            "idx_col_name": "business_id",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "business",
            "idx_col_name": "stars",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "business",
            "idx_col_name": "review_count",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "business",
            "idx_col_name": "state",
            "type": pymongo.TEXT
        },
        {
            "cole_name": "business",
            "idx_col_name": "city",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "review",
            "idx_col_name": "review_id",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "review",
            "idx_col_name": "business_id",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "review",
            "idx_col_name": "user_id",
            "type": pymongo.DESCENDING
        },
        {
            "cole_name": "review",
            "idx_col_name": "text",
            "type": pymongo.TEXT
        }
    ]
    return idx_dict_list
