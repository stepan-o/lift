drop table if exists checkin;

create table checkin
(
    date        text,
    business_id text
);

alter table checkin
    owner to yelp;
