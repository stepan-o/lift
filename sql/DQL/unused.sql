SELECT
    word,
    COUNT(*) AS word_count
FROM (
         SELECT
             regexp_split_to_table(r.text, '\s') as word
         FROM review AS r
                  JOIN business b on r.business_id = b.business_id
         WHERE b.name='Chipotle Mexican Grill'
     ) t
GROUP BY word
ORDER BY word_count DESC
LIMIT 10;
