/* Queries for Yelp database */

/* Lift challenge questions */

-- Question 1 is related to data ingestion

-- Question 2: Top 10 restaurants in Toronto with the highest popularity
-- option 1: most popular = most reviews and stars > 4.4
SELECT
    name,
    city,
    categories,
    review_count,
    stars
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
SELECT
    name,
    city,
    categories,
    review_count,
    stars
FROM business
WHERE
        city='Toronto'
  AND
        categories LIKE '%Restaurants%'
ORDER BY stars DESC, review_count DESC
LIMIT 10;


-- Question 3: How many Canadian residents reviewed the business “Mon Ami Gabi” in last 1 year
---- ********** TODO UNFINISHED
SELECT
    u.user_id,
    u.name,
    COALESCE(rc.rev_can, 0) AS reviews_can,
    u.review_count,
    ROUND(CAST(COALESCE(rc.rev_can, 0) AS DECIMAL) / u.review_count, 2) AS can_perc
FROM yelp_user AS u
         LEFT JOIN (
    SELECT
        u.user_id AS user_id,
        COUNT(*) AS rev_can
    FROM yelp_user AS u
             JOIN review_cleaned r
                  ON u.user_id = r.user_id
             JOIN business b
                  ON r.business_id = b.business_id
    WHERE b.canadian=TRUE
    GROUP BY u.user_id
    HAVING COUNT(*) NOTNULL
) AS rc

                   ON u.user_id = rc.user_id
WHERE u.review_count != 0
ORDER BY can_perc DESC;


-- Question 4: top 10 most common words in the reviews of the business “Chipotle Mexican Grill” might
-- be helpful and interesting to the business.
SELECT * FROM
    ts_stat('SELECT to_tsvector(r.text) FROM review_cleaned AS r JOIN business b on r.business_id = b.business_id WHERE b.name=''Chipotle Mexican Grill''')
ORDER BY nentry DESC
LIMIT 10;

-- Question 5: What’s the percentage of users, who reviewed ​“Mon Ami Gabi”,
-- and also reviewed at least 10 other restaurants located in Ontario?

