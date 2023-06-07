CREATE OR REPLACE FUNCTION incremental_update_sales_rank_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Sales Rank Dimension
	
    drop table if exists cte_sales_rank_dim;
    create temporary table cte_sales_rank_dim as
    
	with new_sales_rank_data as
	(
		select 		sales_rank,
		            sales_rank_category,
		            asin
		from 		staging_db.products p
		where       cast(p.insertion_time as date) >= input_date 
					and sales_rank is not null
	)
	
	select 		product_id as pid,
		            sales_rank_category as src,
					sales_rank as sr
	from 		new_sales_rank_data s
    join 		data_warehouse.product_dim p
    on 			s.asin = p.asin;


	-- Mark the current records as not current
	UPDATE data_warehouse.sales_rank_dim
	SET end_date = CURRENT_DATE
	from cte_sales_rank_dim p
	WHERE sales_rank is not null 
	and product_id = p.pid 
	and sales_rank_category = p.src
	and sales_rank != sr
    AND end_date = '2999-12-31'::date;
	 
	-- Insert the updated record as the current record
	insert into data_warehouse.sales_rank_dim (product_id,sales_rank_category,sales_rank, start_date, end_date)
	select 		pid,src,sr, current_date, '2999-12-31'::date
	from 		cte_sales_rank_dim
    on conflict (product_id, sales_rank_category, sales_rank) do nothing;


END;


$$ LANGUAGE plpgsql;