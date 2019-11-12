drop table if exists yelp_user;

create table yelp_user
(
    fans               integer,
    compliment_cute    integer,
    useful             integer,
    compliment_cool    integer,
    yelping_since      text,
    compliment_funny   integer,
    review_count       integer,
    average_stars      numeric,
    compliment_more    integer,
    funny              integer,
    compliment_list    integer,
    compliment_writer  integer,
    friends            text,
    compliment_hot     integer,
    compliment_plain   integer,
    name               text,
    elite              text,
    compliment_photos  integer,
    compliment_profile integer,
    compliment_note    integer,
    user_id            text not null
        constraint yelp_user_pk
            primary key,
    cool               integer
);

alter table yelp_user
    owner to yelp;

