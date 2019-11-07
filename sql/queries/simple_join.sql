/* Queries for Yelp database */

-- simple join of data from 3 tables

SELECT
    u.name AS user_name,
    b.name AS business,
    r.date,
    r.text
FROM review_cleaned AS r
         JOIN yelp_user as u
              ON r.user_id = u.user_id
         JOIN business b
              ON r.business_id = b.business_id
LIMIT 100;
