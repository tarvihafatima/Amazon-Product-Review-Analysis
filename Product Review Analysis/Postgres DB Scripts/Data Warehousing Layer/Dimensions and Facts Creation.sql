drop table if exists data_warehouse.product_category_dim;
create table data_warehouse.product_category_dim
(
category_id SERIAL  PRIMARY KEY,
category_level int,
category_name text
);
CREATE UNIQUE INDEX category_level_name_idx on data_warehouse.product_category_dim (category_level, category_name);


drop table if exists data_warehouse.price_bucket_dim;
create table data_warehouse.price_bucket_dim
(
bucket_id SERIAL  PRIMARY KEY,
category_id BIGINT references data_warehouse.product_category_dim(category_id),
bucket_name text,
start_range bigint,
end_range bigint
);
CREATE UNIQUE INDEX price_bucket_category_name_idx on data_warehouse.price_bucket_dim (category_id, bucket_name);


-- Create the date dimension table

drop table if exists data_warehouse.date_dim;
CREATE TABLE data_warehouse.date_dim (
  date_id SERIAL PRIMARY KEY,
  date_value DATE,
  day_of_week INT,
  day_of_month INT,
  day_of_year INT,
  month INT,
  month_name VARCHAR(20),
  quarter INT,
  year INT
);

-- Populate the date dimension table with data
INSERT INTO data_warehouse.date_dim (date_value, day_of_week, day_of_month, day_of_year, month, month_name, quarter, year )
SELECT
  d.date_value,
  EXTRACT(DOW FROM d.date_value) AS day_of_week,
  EXTRACT(DAY FROM d.date_value) AS day_of_month,
  EXTRACT(DOY FROM d.date_value) AS day_of_year,
  EXTRACT(MONTH FROM d.date_value) AS month,
  TO_CHAR(d.date_value, 'Month') AS month_name,
  EXTRACT(QUARTER FROM d.date_value) AS quarter,
  EXTRACT(YEAR FROM d.date_value) AS year
FROM
  (
    SELECT generate_series('1996-01-01'::date, '2014-12-31'::date, '1 day'::interval) AS date_value
  ) AS d
 ;


drop table if exists data_warehouse.time_dim;
CREATE TABLE data_warehouse.time_dim (
  time_id SERIAL PRIMARY KEY,
  time_value time,
  hour int,
  minute int,
  second int
);

  insert into data_warehouse.time_dim (time_value, hour, minute, second)
  Select
  t.time_value,
  EXTRACT(HOUR FROM t.time_value) AS hour,
  EXTRACT(MINUTE FROM t.time_value) AS minute,
  EXTRACT(SECOND FROM t.time_value) AS second
   from 
  (SELECT '0:00:00'::time + (sequence.second || ' seconds')::interval AS time_value
		FROM generate_series(0,86399) AS sequence(second)
		GROUP BY sequence.second
)  AS t;

ALTER TABLE data_warehouse.datetime_dim
ADD CONSTRAINT datetime_date_id_idx FOREIGN KEY (date_id) REFERENCES data_warehouse.date_dim (date_id);


ALTER TABLE data_warehouse.datetime_dim
ADD CONSTRAINT datetime_time_id_idx FOREIGN KEY (time_id) REFERENCES data_warehouse.time_dim (time_id);

drop table if exists data_warehouse.datetime_dim;
CREATE TABLE data_warehouse.datetime_dim (
  datetime_id SERIAL PRIMARY KEY,
  datetime_value timestamp,
  unix_time bigint,
  date_id int,
  time_id int
);

 with cte_date as
 (
 	select date_id, date_value from data_warehouse.date_dim 
 	where extract(year from date_value) = extract(year from '2014-01-01'::date)
 ),
 
 cte_time as
 (
    select time_id, time_value from data_warehouse.time_dim 
 )
 
 
 insert into 	data_warehouse.datetime_dim 
				(datetime_value, unix_time, date_id, time_id)
 select 		(date_value || ' ' || time_value)::timestamp as datetime_value,
 				EXTRACT(EPOCH FROM date_value) + EXTRACT(EPOCH FROM time_value) AS unix_time,
 				date_id, time_id
 from           cte_date, cte_time;
 --where          date_id is null or time_id is null
 
drop table if exists data_warehouse.product_dim;
create table data_warehouse.product_dim
(
product_id BIGSERIAL PRIMARY KEY,
asin char(10),
brand text,
title text,
description text,
im_url text,
price numeric(10, 2), 
start_date date,
end_date date
);
CREATE UNIQUE INDEX product_a_b_p_t_idx on data_warehouse.product_dim (asin, brand, price, title);



drop table if exists data_warehouse.product_category_bridge_dim;
create table data_warehouse.product_category_bridge_dim
(
product_category_bridge_id BIGSERIAL primary key,
product_id BIGINT  REFERENCES data_warehouse.product_dim(product_id),
category_id BIGINT references data_warehouse.product_category_dim(category_id)
);
CREATE UNIQUE INDEX product_category_bridge_idx on data_warehouse.product_category_bridge_dim (product_id, category_id);



drop table if exists data_warehouse.sales_rank_dim;
create table data_warehouse.sales_rank_dim
(
sales_rank_id SERIAL PRIMARY KEY,
product_id BIGINT REFERENCES data_warehouse.product_dim(product_id),
sales_rank_category text,
sales_rank bigint,
start_date date,
end_date date
);
CREATE UNIQUE INDEX sales_pid_cat_rank_itx on data_warehouse.sales_rank_dim (product_id, sales_rank_category,sales_rank);


drop table if exists data_warehouse.reviewer_dim;
create table data_warehouse.reviewer_dim
(
reviewer_id BIGSERIAL PRIMARY KEY,
reviewer_uuid varchar(30),
reviewer_name text
);
CREATE UNIQUE INDEX reviewer_uuid_itx on data_warehouse.reviewer_dim (reviewer_uuid);


drop table if exists data_warehouse.review_dim ;
create table data_warehouse.review_dim 
(
review_id BIGSERIAL  PRIMARY KEY,
reviewer_id BIGINT REFERENCES data_warehouse.reviewer_dim(reviewer_id),
product_id BIGINT REFERENCES data_warehouse.product_dim(product_id),
unix_review_time_id BIGINT references data_warehouse.datetime_dim(datetime_id),
review_summary text,
review_text text
);
CREATE UNIQUE INDEX review_rev_prod_time_itx on data_warehouse.review_dim (reviewer_id, product_id, unix_review_time_id);


drop table if exists data_warehouse.also_bought_dim;
CREATE table data_warehouse.also_bought_dim
(
also_bought_id SERIAL PRIMARY KEY,
product_id bigint REFERENCES data_warehouse.product_dim(product_id),
related_product_id bigint REFERENCES data_warehouse.product_dim(product_id)
);
CREATE UNIQUE INDEX also_bought_itx on data_warehouse.also_bought_dim (product_id,related_product_id);


drop table if exists data_warehouse.also_viewed_dim;
CREATE table data_warehouse.also_viewed_dim
(
also_bought_id SERIAL PRIMARY KEY,
product_id bigint REFERENCES data_warehouse.product_dim(product_id),
related_product_id bigint REFERENCES data_warehouse.product_dim(product_id)
);
CREATE UNIQUE INDEX also_viewed_itx on data_warehouse.also_viewed_dim (product_id,related_product_id);


drop table if exists data_warehouse.bought_together_dim;
CREATE table data_warehouse.bought_together_dim
(
also_bought_id SERIAL PRIMARY KEY,
product_id bigint REFERENCES data_warehouse.product_dim(product_id),
related_product_id bigint REFERENCES data_warehouse.product_dim(product_id)
);
CREATE UNIQUE INDEX bought_together_itx on data_warehouse.bought_together_dim (product_id,related_product_id);


drop table if exists data_warehouse.buy_after_view_dim;
CREATE table data_warehouse.buy_after_view_dim
(
also_bought_id SERIAL PRIMARY KEY,
product_id bigint REFERENCES data_warehouse.product_dim(product_id),
related_product_id bigint REFERENCES data_warehouse.product_dim(product_id)
);
CREATE UNIQUE INDEX buy_after_view_itx on data_warehouse.buy_after_view_dim (product_id,related_product_id);


drop table if exists data_warehouse.product_reviews_fact;
CREATE table data_warehouse.product_reviews_fact (
	product_id BIGINT REFERENCES data_warehouse.product_dim(product_id),
	product_category_id INT REFERENCES data_warehouse.product_category_dim(category_id),
	review_id BIGINT REFERENCES data_warehouse.review_dim(review_id),
	reviewer_id BIGINT REFERENCES data_warehouse.reviewer_dim(reviewer_id),
	price_bucket_id INT REFERENCES data_warehouse.price_bucket_dim(bucket_id),
	review_datetime_id BIGINT REFERENCES data_warehouse.datetime_dim(datetime_id),
	sales_rank_id BIGINT REFERENCES data_warehouse.sales_rank_dim(sales_rank_id),
	review_up_votes INT,
	review_interactions INT,
	review_rating numeric(10,1),
	primary key (review_id)
);
