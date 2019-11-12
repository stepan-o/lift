drop table if exists review;

create table review
(
    stars       numeric,
    review_id   text not null
        constraint review_pk
            primary key,
    user_id     text,
    funny       integer,
    text        text,
    date        text,
    useful      integer,
    cool        integer,
    business_id text
);

alter table review
    owner to yelp;

