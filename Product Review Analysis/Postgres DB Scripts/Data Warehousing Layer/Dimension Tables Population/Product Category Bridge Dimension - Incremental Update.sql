CREATE OR REPLACE FUNCTION incremental_product_category_bridge_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Also Bought Dimension
	
	drop table if exists cte_product_category_bridge_dim;
    create temporary table cte_product_category_bridge_dim as
    
	with new_prod_cat_data as 
	(
		select 		asin,
					category_level
		from 		staging_db.products
		where       cast(insertion_time as date) >= input_date 
		            and asin is not null 
		group by	asin, category_level
		
	)
	
	select 		product_id pid,
				category_id cid
				from data_warehouse.product_dim pd
				left join new_prod_cat_data npcd 
				on pd.asin = npcd.asin
				left join data_warehouse.product_category_dim pcd 
				on pcd.category_level = npcd.category_level;
			
			
    delete from cte_product_category_bridge_dim
    using data_warehouse.product_category_bridge_dim pcbd
    where pcbd.product_id = pid and category_id is null;
	
	insert into 	data_warehouse.product_category_bridge_dim  (product_id,category_id)
	select 			pid,cid
	from 			cte_product_category_bridge_dim
	on conflict 	(product_id,category_id) DO NOTHING;
	
	
END;
$$ LANGUAGE plpgsql;

