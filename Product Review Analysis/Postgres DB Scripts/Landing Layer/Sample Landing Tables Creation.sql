-- landing_db.products definition

-- Drop table

-- DROP TABLE landing_db.products;

CREATE TABLE landing_db.products_temp (
	asin text NULL,
	brand text NULL,
	categories text NULL,
	description text NULL,
	im_url text NULL,
	price text NULL,
	related text NULL,
	sales_rank text NULL,
	title text NULL
);

-- landing_db.reviews definition

-- Drop table

-- DROP TABLE landing_db.reviews;

CREATE TABLE landing_db.reviews_temp (
	asin text NULL,
	helpful text NULL,
	overall text NULL,
	review_text text NULL,
	review_time text NULL,
	reviewer_id text NULL,
	reviewer_name text NULL,
	summary text NULL,
	unix_review_time text NULL
);

insert into landing_db.reviews_temp select * from landing_db.reviews limit 10000;
insert into landing_db.products_temp select * from landing_db.products limit 10000;

commit
