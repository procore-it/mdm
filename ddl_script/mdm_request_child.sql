create table if not exists mdm_request_child(
  request_child_id SERIAL primary key,
  request_parent_id int  references mdm_request (request_id),
  folder_name varchar(500) not null,
  created_timestamp       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  pii_columns_list        varchar(1000)
);