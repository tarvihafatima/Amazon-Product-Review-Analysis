CREATE OR REPLACE FUNCTION data_warehouse.incremental_update_price_bucket_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	

-- Populate Price Bucket Dimension
	
	with new_products_data as 
	(
	    select 		p.category_level,p.category_name, p.price
		from 		staging_db.products p
		where       category_level = 0 and cast(p.insertion_time as date) >= input_date 
	)
	,
	
	cte_category_id_price as 
	(
		select 		pcd.category_id, p.price
		from 		data_warehouse.product_category_dim pcd
		join 		new_products_data p
		on 			pcd.category_level = p.category_level and
		            pcd.category_name = p.category_name
	
	)
	,
	
	cte_categoty_stats as 
	(
		select		category_id,
		            cast(min(price) as int) as min_price,
		            cast(max(price) as int) as max_price,
		            cast(avg(price) as int) as avg_price,
		            case when cast(max(price) - min(price) as int) = 0 
		            	then 0 
		            	else 1 
		            end as has_bucket
		from 		cte_category_id_price p 
		group by	1
	)
	,
	
	bucket_ranges_cols as 
	(
		select 		category_id,
					has_bucket,
					case when has_bucket = 1 
						then 0
				   		else -1 
				    end as low_start_range,
				    
					case when has_bucket = 1 
						then min_price + cast((avg_price - min_price)/ 2 as int)
				    	else -1 
				    end as low_end_range,
				    
				    case when has_bucket = 1 
						then min_price + cast((avg_price - min_price)/ 2 as int) +1
				    	else -1 
				    end as medium_start_range,
				    
				    case when has_bucket = 1 
						then max_price - (cast((max_price - avg_price)/ 2 as int)) -1
				    	else -1 
				    end as medium_end_range,
				    
				    case when has_bucket = 1 
						then max_price - (cast((max_price - avg_price)/ 2 as int)) 
				    	else -1 
				    end as high_start_range,
				    
				    case when has_bucket = 1 
						then max_price
				    	else -1 
				    end as high_end_range
						
		from 		cte_categoty_stats
	  )
	  ,
	  
	  bucket_ranges_rows as
	  (
	  	select 			category_id, 
	  					'Low' as bucket_name, 
	  					low_start_range as start_range, 
	  					low_end_range as end_range
	  	from			bucket_ranges_cols
	  		 	
	  	union 

	  	select 			category_id, 
	  					'Medium' as bucket_name, 
	  					medium_start_range as start_range, 
	  					medium_end_range as end_range
	  	from			bucket_ranges_cols
	  		 	
	  	union 

	  	select 			category_id, 
	  					'High' as bucket_name, 
	  					high_start_range as start_range, 
	  					high_end_range as end_range	
	  	from			bucket_ranges_cols
	  )
	  
	  
	insert into 	data_warehouse.price_bucket_dim (category_id,bucket_name, start_range, end_range)
	select 			category_id,bucket_name, start_range, end_range
	from 			bucket_ranges_rows
	on conflict 	(category_id, bucket_name) 
	DO UPDATE 
	set 			start_range =excluded.start_range,
					end_range = excluded.end_range;
end;
$$ LANGUAGE plpgsql;


