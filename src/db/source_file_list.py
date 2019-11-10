import os


def source_file_list():
    data_path = os.path.join('data', 'yelp_dataset')
    business_path = os.path.join(data_path, 'business.json')
    user_path = os.path.join(data_path, 'user.json')
    review_path = os.path.join(data_path, 'review.json')
    ingest_business = {'file_path': business_path, 'target_table': 'business'}
    ingest_user = {'file_path': user_path, 'target_table': 'yelp_user'}
    ingest_review = {'file_path': review_path, 'target_table': 'review'}
    ing_list = [ingest_business, ingest_user, ingest_review]
    return ing_list
