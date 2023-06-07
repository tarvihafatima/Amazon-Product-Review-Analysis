-- Analysis by Prodcut Category


-- Most Positive Reviewed Products per Prodcut Category
drop view if exists data_warehouse.most_positive_reviewed_products;
create  view data_warehouse.most_positive_reviewed_products as
with products_reviews as
(
	select pcd.category_name, pd.asin , avg(review_rating) as avg_review_rating
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	join data_warehouse.review_dim rd
	on rd.review_id = prf.review_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null
	group by 1, 2
)

,
products_reviews_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY avg_review_rating DESC) AS rn
	from products_reviews prc
)

select category_name, asin, avg_review_rating
	from products_reviews_rn prc
	where rn = 1
	limit 10;


-- Most Negative Reviewed Products per Prodcut Category
drop view if exists data_warehouse.most_negative_reviewed_products;
create  view data_warehouse.most_negative_reviewed_products as
with products_reviews as
(
	select pcd.category_name, pd.asin , avg(review_rating) as avg_review_rating
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	join data_warehouse.review_dim rd
	on rd.review_id = prf.review_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null
	group by 1, 2
)

,
products_reviews_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY avg_review_rating ASC) AS rn
	from products_reviews prc
)

select category_name, asin, avg_review_rating
	from products_reviews_rn prc
	where rn = 1
	limit 10;


-- Most Positive Reviewed Brands per Prodcut Category
drop view if exists data_warehouse.most_positive_reviewed_brands;
create  view data_warehouse.most_positive_reviewed_brands as
with products_reviews as
(
	select pcd.category_name, pd.brand , avg(review_rating) as avg_review_rating
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	join data_warehouse.review_dim rd
	on rd.review_id = prf.review_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null
	group by 1, 2
)

,
products_reviews_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY avg_review_rating DESC) AS rn
	from products_reviews prc
)

select category_name, brand, avg_review_rating
	from products_reviews_rn prc
	where rn = 1
	limit 10;


-- Most Negative Reviewed Brands per Prodcut Category
drop view if exists data_warehouse.most_negative_reviewed_brands;
create  view data_warehouse.most_negative_reviewed_brands as
with products_reviews as
(
	select pcd.category_name, pd.brand , avg(review_rating) as avg_review_rating
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	join data_warehouse.review_dim rd
	on rd.review_id = prf.review_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null
	group by 1, 2
)

,
products_reviews_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY avg_review_rating ASC) AS rn
	from products_reviews prc
)

select category_name, brand, avg_review_rating
	from products_reviews_rn prc
	where rn = 1
	limit 10;

-- Most Reviewed Products per Prodcut Category

drop view if exists data_warehouse.most_reviewed_products;
create  view data_warehouse.most_reviewed_products as
with products_reviews_count as
(
	select pcd.category_name, pd.asin , count(review_id) as reviews_count
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null
	group by 1, 2
)

,
products_reviews_count_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY reviews_count DESC) AS rn
	from products_reviews_count prc
)

select category_name, asin, reviews_count
	from products_reviews_count_rn prc
	where rn = 1
	limit 10;


-- Most Reviewed Brands per Prodcut Category

drop view if exists data_warehouse.most_reviewed_brands;
create  view data_warehouse.most_reviewed_brands as
with products_reviews_count as
(
	select pcd.category_name, pd.brand, count(review_id) as reviews_count
	from data_warehouse.product_dim pd
	join data_warehouse.product_reviews_fact prf
	on pd.product_id = prf.product_id
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
    where category_name is not null and brand is not null
	group by 1, 2
)
,
products_reviews_count_rn as
(
	select *,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY reviews_count DESC) AS rn
	from products_reviews_count prc
)

select category_name, brand, reviews_count
	from products_reviews_count_rn prc
	where rn = 1
	limit 10;



-- Most High-end Prodcuts for each Prodcut Category
drop view if exists data_warehouse.most_high_end_products;
create  view data_warehouse.most_high_end_products as

with cte_product_categories_price as
(
	select category_name, asin, price,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY price DESC) AS rn
	from data_warehouse.product_dim pd
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
	left join data_warehouse.price_bucket_dim pbd
	on pbd.category_id = pcd.category_id
	and pbd.start_range <= pd.price
	and pbd.end_range >= pd.price
	where pbd.bucket_name = 'High' and brand is not null
)


select category_name, asin as product_asin, price
from cte_product_categories_price hep
where rn = 1
limit 5;



-- Most Low_Cost Prodcuts for each Prodcut Category

drop view if exists data_warehouse.most_low_cost_products;
create  view data_warehouse.most_low_cost_products as

with cte_product_categories_price as
(
	select category_name, asin, price,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY price ASC) AS rn
	from data_warehouse.product_dim pd
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
	left join data_warehouse.price_bucket_dim pbd
	on pbd.category_id = pcd.category_id
	and pbd.start_range <= pd.price
	and pbd.end_range >= pd.price
	where pbd.bucket_name = 'Low' and price <> 0 and brand is not null
)


select category_name, asin as product_asin, price
from cte_product_categories_price hep
where rn = 1
limit 10;


-- Most High-end Brands for each Prodcut Category
drop view if exists data_warehouse.most_high_end_brands;
create  view data_warehouse.most_high_end_brands as

with cte_product_categories_price as
(
	select category_name, brand , price,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY price DESC) AS rn
	from data_warehouse.product_dim pd
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
	left join data_warehouse.price_bucket_dim pbd
	on pbd.category_id = pcd.category_id
	and pbd.start_range <= pd.price
	and pbd.end_range >= pd.price
	where pbd.bucket_name = 'High' and brand is not null
)


select category_name, brand , price
from cte_product_categories_price hep
where rn = 1
limit 10;



-- Most Low_Cost Brands for each Prodcut Category
drop view if exists data_warehouse.most_low_cost_brands;
create  view data_warehouse.most_low_cost_brands as

with cte_product_categories_price as
(
	select category_name, brand, price,
	ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY price ASC) AS rn
	from data_warehouse.product_dim pd
	join data_warehouse.product_category_bridge_dim pcbd 
	on pd.product_id = pcbd.product_id
	left join data_warehouse.product_category_dim pcd 
    on pcbd.category_id = pcbd.category_id
	left join data_warehouse.price_bucket_dim pbd
	on pbd.category_id = pcd.category_id
	and pbd.start_range <= pd.price
	and pbd.end_range >= pd.price
	where pbd.bucket_name = 'Low' and price <> 0 and (brand is not null)
)

select category_name, brand, price
from cte_product_categories_price hep
where rn = 1 
limit 10;

