drop table if exists tip;

create table tip
(
    date             text,
    business_id      text,
    text             text,
    compliment_count integer,
    user_id          text
);

alter table tip
    owner to yelp;
