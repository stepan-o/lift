/* Queries for Yelp database */

/* Lift challenge questions */

-- Question 1 is related to data ingestion, see sql/DDL and /DML folders

-- Question 2: Top 10 restaurants in Toronto with the highest popularity
-- option 1: most popular = most reviews and stars > 4.4
SELECT name, review_count, stars
FROM business
WHERE
      city='Toronto'
  AND
        categories LIKE '%Restaurants%'
  AND
        stars > 4.4
ORDER BY review_count DESC, stars DESC
LIMIT 10;

-- option 2: most popular = highest rated, ordered by review count
SELECT name, review_count, stars
FROM business
WHERE
        city='Toronto'
  AND
        categories LIKE '%Restaurants%'
ORDER BY stars DESC, review_count DESC
LIMIT 10;


-- Question 3: How many Canadian residents reviewed the business “Mon Ami Gabi” in last 1 year
SELECT
    COUNT(DISTINCT can_users.can_user_id)
FROM (
         SELECT
             r.user_id can_user_id,
             -- compute the ratio of reviews in Canadian provinces vs total reviews
             COALESCE(SUM(CASE WHEN b.state IN
                                    ('NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU')
                                   THEN 1 ELSE 0 END), 0) / CAST(COUNT(*) AS DECIMAL) can_rev_ratio
         FROM review r
                  JOIN business b
                       ON r.business_id = b.business_id
         GROUP BY r.user_id
                  -- only select Canadian residents: users with > 60% of reviews recorded in Canada
         HAVING COALESCE(SUM(CASE WHEN b.state IN
                                       ('NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU')
                                      THEN 1 ELSE 0 END), 0) / CAST(COUNT(*) AS DECIMAL) > 0.6
     ) AS can_users
         INNER JOIN review r2
                    ON r2.user_id = can_users.can_user_id
         JOIN business b2
              ON r2.business_id = b2.business_id
WHERE b2.name = 'Mon Ami Gabi'
  AND r2.date LIKE '2018%';


-- Question 4: top 10 most common words in the reviews of the business “Chipotle Mexican Grill” might
-- be helpful and interesting to the business.
SELECT * FROM
    ts_stat('SELECT to_tsvector(r.text) FROM review AS r JOIN business b on r.business_id = b.business_id WHERE b.name=''Chipotle Mexican Grill''')
ORDER BY nentry DESC
LIMIT 10;


-- Question 5: What’s the percentage of users, who reviewed ​“Mon Ami Gabi”,
-- and also reviewed at least 10 other restaurants located in Ontario?

