--create mdm table
--This table holds all tha values that are required to delete the expired destination s3 bucket and 
--send e-mail notification before 5 days of s3 bucket expiry

create table if not exists mdm
(
    request_id              int unsigned auto_increment primary key,
    system_name             varchar(255)    not null,
    source_bucket           varchar(500)    not null,
    destination_bucket      varchar(500)    not null,
    persona                 varchar(255)    not null,
    expiration_date         DATE            not null,
    created_timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_disabled             TINYINT(0),
    email                   varchar(255)    not null,
    deletion_notice_sent    TINYINT(0),
    pii_columns_list        varchar(1000),
    deleted_timestamp       TIMESTAMP
);

