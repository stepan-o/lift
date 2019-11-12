/* ------------------------------------------------------------------------------------- */
/* ----------------------------- Queries for Yelp database ----------------------------- */
/* ------------------------------------------------------------------------------------- */
/* ------------------------------ Lift challenge questions ------------------------------*/
/* --------------------------------------------------------------------------------------*/

-- In addition to questions 6 and 7, as a part of EDA of the dataset, word clouds have
-- been produced in Python for all reviews grouped by star rating (1-star, 2-star, etc.).
-- See folder notebooks/eda_results and notebook yelp_explore.ipynb

/* ------------------------------------ Question 1 ------------------------------------- */
-- Question 1 is related to data ingestion, see sql/DDL and sql/DML folders for table
-- definition; Python scripts for PostgreSQL and MongoDB database setup can be found in
-- the folder src/

/* ------------------------------------ Question 2 ------------------------------------- */
-- Top 10 restaurants in Toronto with the highest popularity
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

/* ------------------------------------ Question 3 ------------------------------------- */
-- How many Canadian residents reviewed the business “Mon Ami Gabi” in last 1 year?
SELECT
-- number of Canadian residents (>60% reviews in Canada) who reviewed Mon Ami Gabi in the past year
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

/* ------------------------------------ Question 4 ------------------------------------- */
-- The top 10 most common words in the reviews of the business “Chipotle Mexican Grill”
SELECT * FROM
    ts_stat('SELECT to_tsvector(r.text) FROM review AS r JOIN business b on r.business_id = b.business_id WHERE b.name=''Chipotle Mexican Grill''')
ORDER BY nentry DESC
LIMIT 10;

/* ------------------------------------ Question 5 ------------------------------------- */
-- What’s the percentage of users, who reviewed ​“Mon Ami Gabi”,
-- and also reviewed at least 10 other restaurants located in Ontario?

WITH mag_user_count AS (
    -- get the number of users who reviewed Mon Ami Gabi
    SELECT COUNT(DISTINCT r3.user_id) AS num_mag_reviewers
    FROM review r3
             JOIN business b3 ON r3.business_id = b3.business_id
    WHERE b3.name = 'Mon Ami Gabi'
)

SELECT
       -- number of users who reviewed Mon Abi Gabi who also reviewed at least 10 restaurants in Ontario
    COUNT(*) AS users_mag_ont10,
       -- number of users who reviewed Mon Ami Gabi
    muc.num_mag_reviewers AS mag_reviewers,
       -- percentage of users who reviewed Mon Abi Gabi who also reviewed at least 10 restaurants in Ontario
    CAST(COUNT(*) AS DECIMAL) / muc.num_mag_reviewers AS ont10_to_mag_ratio
FROM (
         -- ...who also reviewed at least 10 different restaurants in Ontario
         SELECT mr.mag_reviewers AS user_id
         FROM (
                  -- users who reviewed Mon Ami Gabi...
                  SELECT DISTINCT r.user_id as mag_reviewers
                  FROM yelp.public.review r
                           JOIN business b
                                on r.business_id = b.business_id
                  WHERE b.name = 'Mon Ami Gabi'
              ) AS mr
                  JOIN review r2
                       ON r2.user_id = mr.mag_reviewers
                  JOIN business b2
                       ON b2.business_id = r2.business_id
           -- at least 10 different restaurants reviewed in Ontario
         WHERE b2.state = 'ON'
           AND b2.categories LIKE '%Restaurants%'
         GROUP BY mr.mag_reviewers
         HAVING COUNT(DISTINCT b2.business_id) >= 10
     ) AS mag_ont10
         JOIN mag_user_count muc
              ON true
-- add variable with total count of users who reviewed Mon Ami Gabi to the GROUP BY clause
GROUP BY muc.num_mag_reviewers;

/* ------------------------------------ Question 6 ------------------------------------- */
-- Most frequent bi-grams (with and without stop words) for 5-star reviews of Starbucks
-- could be used to investigate phrases associated with 5-star reviews of Starbucks
WITH word_list AS (
    SELECT
        review_id    AS review_id,
        -- select one of the two options for text vectorization: keep stop words, or remove them (uncomment one)
        -- option 1: convert review text to array, remove stop words
        tsvector_to_array(to_tsvector('english', text)) AS review_array
        -- option 2: convert review text to array, keep stop words
        -- string_to_array(regexp_replace(lower(text), E'[^a-z0-9_]+', ' ', 'g'), ' ') AS review_array
    FROM review
             JOIN business
                  ON review.business_id = business.business_id
    WHERE business.name = 'Starbucks'
      AND review.stars = 5
),
     word_indexes AS (
         SELECT review_id,
                review_array,
                generate_subscripts(review_array, 1)
                    AS word_id
         FROM word_list
     ),
     numbered_words AS (
         SELECT review_id,
                review_array[word_id] word,
                word_id
         FROM word_indexes
     )
SELECT
    nw1.word,
    nw2.word,
    count(1)
FROM numbered_words nw1
         JOIN numbered_words nw2 ON
            nw1.word_id = nw2.word_id - 1
        AND nw1.review_id = nw2.review_id
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 30;

/* ------------------------------------ Question 7 ------------------------------------- */
-- Most frequent bi-grams (with and without stop words) for 5-star reviews of Starbucks
-- could be useful to determine strong attractors for people and reward staff at outstanding locations
SELECT
    b.name,
    b.city,
    b.address,
    b.stars AS avg_rating,
    r.stars AS select_review_rating,
    r.text AS select_review_text
FROM review r
         JOIN business b
              ON r.business_id = b.business_id
WHERE b.name = 'Starbucks'
  AND r.stars = 5
  AND lower(r.text) LIKE '%my favourite%'
ORDER BY b.city;

