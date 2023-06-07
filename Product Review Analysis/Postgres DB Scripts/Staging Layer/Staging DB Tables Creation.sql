-- staging_db.product definition

-- Drop table

DROP TABLE staging_db.products;

CREATE TABLE staging_db.products (
  asin CHAR(10) ,
  brand TEXT,
  category_level INT,
  category_name TEXT,
  description TEXT,
  im_url TEXT,
  price NUMERIC(10, 2),
  also_viewed CHAR(10)[],
  also_bought CHAR(10)[],
  bought_together CHAR(10)[],
  buy_after_view CHAR(10)[],
  sales_rank BIGINT,
  sales_rank_category TEXT,
  title TEXT,
  insertion_time TIMESTAMP
  );
 
  
 -- landing_db.reviews definition

-- Drop table

DROP TABLE staging_db.reviews

CREATE TABLE staging_db.reviews (
	asin CHAR(10) ,
	review_up_votes INT ,
	review_interactions INT,
	review_rating NUMERIC(10, 1) ,
	review_text TEXT ,
	review_date DATE ,
	reviewer_id CHAR(30) ,
	reviewer_name TEXT ,
	review_summary TEXT ,
	unix_review_time BIGINT ,
    insertion_time TIMESTAMP
);