CREATE OR REPLACE FUNCTION incremental_update_also_bought_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Also Bought Dimension
	
	with new_also_bought_data as 
	(
		select		asin,
		            also_bought
		from 		staging_db.products
		where       cast(insertion_time as date) >= input_date
		            and also_bought is not null
	),
	
	cte_also_bought_dim_1 as 
	(
        select      asin,
    				unnest(also_bought) as related_product_asin
    				from new_also_bought_data
	),
	
	cte_also_bought_dim as 
	(
        select      pcd1.product_id,
    				pcd2.product_id as related_product_id
    				from cte_also_bought_dim_1 cabd
    				join data_warehouse.product_dim pcd1 
    				on   cabd.asin = pcd1.asin
    				join data_warehouse.product_dim pcd2
    				on   cabd.related_product_asin = pcd2.asin
	)
	
	insert into 	data_warehouse.also_bought_dim (product_id,related_product_id)
	select 			product_id,related_product_id
	from 			cte_also_bought_dim
	on conflict 	(product_id,related_product_id) DO NOTHING;
	
	
END;
$$ LANGUAGE plpgsql;
