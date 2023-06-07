CREATE OR REPLACE FUNCTION incremental_update_product_review_fact(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Product Review Fact 
	
	with filtered_reviews as 
	(
			select * 
			from staging_db.reviews r 
			where
			cast(insertion_time as date) >= input_date 
			and asin is not null and reviewer_id is not null and unix_review_time is not null
	),
	
	cte_review_facts as
	(
		select 	rd.review_id,
				rrd.reviewer_id,
				pd.product_id,
				rd.unix_review_time_id,
				r.review_up_votes, 
				r.review_interactions, 
				r.review_rating
				from filtered_reviews r 
				join data_warehouse.reviewer_dim rrd
				on rrd.reviewer_uuid = r.reviewer_id
				join data_warehouse.product_dim pd 
				on pd.asin = r.asin
				join data_warehouse.review_dim rd 
				on rd.reviewer_id = rrd.reviewer_id
				and rd.product_id = pd.product_id
				join data_warehouse.datetime_dim dd 
				on dd.datetime_id = rd.unix_review_time_id
				and dd.unix_time = r.unix_review_time
	)
	
	insert into 	data_warehouse.product_reviews_fact (product_id,product_category_id, review_id, reviewer_id,
	                                                     price_bucket_id,review_datetime_id, sales_rank_id, 
	                                                     review_up_votes, review_interactions, review_rating)
	select 			pd.product_id, pcd.category_id, rd.review_id, rrd.reviewer_id,
	                pbd.bucket_id, dtd.datetime_id as review_datetime_id, srd.sales_rank_id, 
	                rd.review_up_votes, rd.review_interactions, rd.review_rating
	from 			cte_review_facts rd
					
					join data_warehouse.product_dim  pd
					on pd.product_id = rd.product_id 

					join data_warehouse.product_category_bridge_dim pcbd 
					on pd.product_id = pcbd.product_id
					
					join data_warehouse.reviewer_dim rrd
					on rrd.reviewer_id = rd.reviewer_id
					
					join data_warehouse.datetime_dim dtd
					on dtd.datetime_id = rd.unix_review_time_id
					
					left join data_warehouse.sales_rank_dim srd
					on srd.product_id = pd.product_id
					
					left join data_warehouse.product_category_dim pcd 
					on pcbd.category_id = pcbd.category_id
					
					left join data_warehouse.price_bucket_dim pbd
					on pbd.category_id = pcd.category_id
					and pbd.start_range <= pd.price
					and pbd.end_range >= pd.price
					
	
	on conflict 	(review_id) do nothing;
	

END;
$$ LANGUAGE plpgsql;

