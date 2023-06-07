create database amazonreviews;

create schema landing_db;
create schema staging_db;
create schema data_warehouse;


drop table if exists Job_Stages_Log;
create table Job_Stages_Log
(
    log_id serial primary KEY,
	sourcing_time timestamp,
	transformation_time timestamp,
	warehousing_time timestamp
);
