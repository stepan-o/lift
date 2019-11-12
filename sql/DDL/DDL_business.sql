drop table if exists business;

create table business
(
    "attributes.Alcohol"                    text,
    hours                                   text,
    "hours.Thursday"                        text,
    review_count                            integer,
    stars                                   numeric,
    "attributes.RestaurantsGoodForGroups"   text,
    name                                    text,
    "attributes.BikeParking"                text,
    "hours.Tuesday"                         text,
    "attributes.GoodForDancing"             text,
    "attributes.Open24Hours"                text,
    "attributes.RestaurantsPriceRange2"     text,
    postal_code                             text,
    "attributes.Corkage"                    text,
    longitude                               numeric,
    "attributes.AgesAllowed"                text,
    "hours.Friday"                          text,
    "attributes.BYOBCorkage"                text,
    "attributes.HappyHour"                  text,
    "attributes.RestaurantsTableService"    text,
    is_open                                 integer,
    "hours.Sunday"                          text,
    "attributes.WheelchairAccessible"       text,
    "attributes.WiFi"                       text,
    "attributes.AcceptsInsurance"           text,
    state                                   text,
    "attributes.CoatCheck"                  text,
    "attributes.BusinessAcceptsBitcoin"     text,
    "attributes.BestNights"                 text,
    "attributes.HairSpecializesIn"          text,
    "attributes.DietaryRestrictions"        text,
    "attributes.OutdoorSeating"             text,
    "attributes.NoiseLevel"                 text,
    "attributes.RestaurantsAttire"          text,
    "attributes.GoodForMeal"                text,
    "attributes.BYOB"                       text,
    "hours.Monday"                          text,
    "attributes.RestaurantsCounterService"  text,
    latitude                                numeric,
    "attributes.GoodForKids"                text,
    categories                              text,
    "attributes.Music"                      text,
    "attributes.Smoking"                    text,
    business_id                             text not null
        constraint business_pk
            primary key,
    "attributes.Caters"                     text,
    "attributes.DriveThru"                  text,
    "attributes.ByAppointmentOnly"          text,
    city                                    text,
    "attributes.HasTV"                      text,
    "hours.Saturday"                        text,
    "attributes.Ambience"                   text,
    "attributes.DogsAllowed"                text,
    "attributes.RestaurantsTakeOut"         text,
    "hours.Wednesday"                       text,
    "attributes.BusinessParking"            text,
    "attributes.BusinessAcceptsCreditCards" text,
    "attributes.RestaurantsReservations"    text,
    "attributes.RestaurantsDelivery"        text,
    address                                 text,
    attributes                              text
);

alter table business
    owner to yelp;

