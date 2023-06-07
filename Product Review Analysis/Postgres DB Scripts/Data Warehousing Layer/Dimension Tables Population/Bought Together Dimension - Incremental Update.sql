CREATE OR REPLACE FUNCTION incremental_update_bought_together_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Bought Together Dimension
	
	with new_bought_together_data as 
	(
		select		asin,
		            bought_together
		from 		staging_db.products
		where       cast(insertion_time as date) >= input_date 
					and bought_together is not null
	),
	
	cte_bought_together_dim_1 as 
	(
        select      asin,
    				unnest(bought_together) as related_product_asin
    				from new_bought_together_data
	),
	
	cte_bought_together_dim as 
	(
        select      pcd1.product_id,
    				pcd2.product_id as related_product_id
    				from cte_bought_together_dim_1 cabd
    				join data_warehouse.product_dim pcd1 
    				on   cabd.asin = pcd1.asin
    				join data_warehouse.product_dim pcd2
    				on   cabd.related_product_asin = pcd2.asin
	)
	
	insert into 	data_warehouse.bought_together_dim (product_id,related_product_id)
	select 			product_id,related_product_id
	from 			cte_bought_together_dim
	on conflict 	(product_id,related_product_id) DO NOTHING;
	
	
END;
$$ LANGUAGE plpgsql;
