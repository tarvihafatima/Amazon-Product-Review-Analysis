CREATE OR REPLACE FUNCTION incremental_update_product_category_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Product Category Dimension
	
	with cte_product_category_dim as 
	(
		select		category_level,
					category_name 
		from 		staging_db.products
		where       cast(insertion_time as date) >= input_date 
		            and category_level is not null
		group by	category_level,
					category_name 
	)
	
	insert into 	data_warehouse.product_category_dim (category_level,category_name)
	select 			category_level,category_name
	from 			cte_product_category_dim
	on conflict 	(category_level, category_name) do nothing;
	
	
	
END;
$$ LANGUAGE plpgsql;
