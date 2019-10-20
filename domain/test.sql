create table visits
(
    id         serial  not null
        constraint visits_pk
            primary key,
    netloc     varchar not null,
    time_stamp timestamp with time zone default CURRENT_TIMESTAMP,
    url        varchar not null
);

alter table visits
    owner to root;

create index visits_netloc_index
    on visits (netloc);

create table page
(
    id     serial  not null
        constraint page_pk
            primary key,
    netloc varchar not null,
    url    varchar not null
);

alter table page
    owner to root;


create table page_x_page
(
    created_date     timestamp with time zone default CURRENT_TIMESTAMP not null,
    found_on_page_id integer                                            not null
        constraint page_x_page_page_id_fk
            references page,
    page_id          integer                                            not null
        constraint page_x_page_page_id_fk_2
            references page
);

alter table page_x_page
    owner to root;

create table queue
(
    id        integer not null
        constraint queue_pk
            primary key,
    url       varchar not null,
    scheduled timestamp with time zone default CURRENT_TIMESTAMP
);

alter table queue
    owner to root;

alter table queue
	add netloc varchar not null;

create sequence queue_id_seq;

alter table queue alter column id set default nextval('public.queue_id_seq');

alter sequence queue_id_seq owned by queue.id;

