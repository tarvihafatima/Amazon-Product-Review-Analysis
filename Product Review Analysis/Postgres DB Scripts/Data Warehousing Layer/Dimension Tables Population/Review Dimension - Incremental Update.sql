CREATE OR REPLACE FUNCTION incremental_update_review_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Review Dimension
	
	with new_review_data as 
	(
		select		review_summary,
		            review_text,
		            asin,
		            unix_review_time,
		            reviewer_id
		from 		staging_db.reviews
		where       cast(insertion_time as date) >= input_date 
		            and asin is not null and reviewer_id is not null and unix_review_time is not null
	)
	,
	
	cte_review_dim as 
	(
		select		review_summary,
		            review_text,
		            product_id,
		            rd.reviewer_id,
		            dd.datetime_id as unix_review_time_id
		from 		new_review_data r
					join data_warehouse.reviewer_dim rd
					on rd.reviewer_uuid = r.reviewer_id
					join data_warehouse.product_dim p
					on p.asin = r.asin
					join data_warehouse.datetime_dim dd 
					on dd.unix_time = r.unix_review_time
	)
	
	insert into 	data_warehouse.review_dim (reviewer_id,product_id, unix_review_time_id, review_summary,review_text)
	select 			reviewer_id,product_id, unix_review_time_id, review_summary,review_text
	from 			cte_review_dim
	on conflict 	(reviewer_id, product_id, unix_review_time_id) do nothing;
	
	
END;
$$ LANGUAGE plpgsql;
