alter table review
    add constraint review_yelp_user_user_id_fk
        foreign key (user_id) references yelp_user;

alter table review
    add constraint review_business_business_id_fk
        foreign key (business_id) references business;
