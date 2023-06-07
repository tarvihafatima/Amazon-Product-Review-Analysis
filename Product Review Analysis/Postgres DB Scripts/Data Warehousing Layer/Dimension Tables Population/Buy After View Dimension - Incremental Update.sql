CREATE OR REPLACE FUNCTION incremental_update_buy_after_view_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Buy After View Dimension
	
	with new_buy_after_view_data as 
	(
		select		asin,
		            buy_after_view
		from 		staging_db.products
		where       cast(insertion_time as date) >= input_date
					and buy_after_view is not null
	),
	
	cte_buy_after_view_dim_1 as 
	(
        select      asin,
    				unnest(buy_after_view) as related_product_asin
    				from new_buy_after_view_data
	),
	
	cte_buy_after_view_dim as 
	(
        select      pcd1.product_id,
    				pcd2.product_id as related_product_id
    				from cte_buy_after_view_dim_1 cabd
    				join data_warehouse.product_dim pcd1 
    				on   cabd.asin = pcd1.asin
    				join data_warehouse.product_dim pcd2
    				on   cabd.related_product_asin = pcd2.asin
	)
	
	insert into 	data_warehouse.buy_after_view_dim (product_id,related_product_id)
	select 			product_id,related_product_id
	from 			cte_buy_after_view_dim
	on conflict 	(product_id,related_product_id) DO NOTHING;
	
	
END;
$$ LANGUAGE plpgsql;