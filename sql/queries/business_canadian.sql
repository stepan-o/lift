ALTER TABLE business
    ADD COLUMN canadian BOOLEAN;

UPDATE business
    SET canadian = TRUE
    WHERE state IN ('NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB', 'BC', 'YT', 'NT', 'NU');

ALTER TABLE yelp.public.yelp_user
    ADD COLUMN reviews_ca INT;

ALTER TABLE yelp.public.yelp_user
    ADD COLUMN reviews_nonca INT;
