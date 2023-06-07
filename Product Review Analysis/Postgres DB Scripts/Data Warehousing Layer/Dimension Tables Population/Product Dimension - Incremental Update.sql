CREATE OR REPLACE FUNCTION incremental_update_product_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Product Dimension
	
    drop table if exists cte_product_dim;
    create temporary table cte_product_dim as
    
    select 		    asin as product_asin,
					brand as product_brand,
					title AS product_title,
					description as product_description,
					im_url as product_im_url,
					price as product_price,
					insertion_time
					
	from 		staging_db.products p
	where       cast(p.insertion_time as date) >= input_date 
                and asin is not null
    union
   
        
    select 		    asin as product_asin,
					null as product_brand,
					NULL AS product_title,
					null as product_description,
					null as product_im_url,
					null as product_price,
					insertion_time
					
	from 		staging_db.reviews r
	where       cast(r.insertion_time as date) >= current_date - 2
                and asin is not null;


    -- Delete duplicate records from new data
    delete from cte_product_dim p
    using data_warehouse.product_dim
    where 
    (asin = p.product_asin) 
    and (brand = p.product_brand or brand is null)
    and (description = p.product_title or product_title is null)
    and (description = p.product_description or product_description is null)
    and (im_url = p.product_im_url or product_im_url is  null)
    and (price = p.product_price or product_price is  null);
    	
	-- Mark the current records as not current
	UPDATE data_warehouse.product_dim
	SET end_date = CURRENT_DATE
	from cte_product_dim p
	WHERE asin = p.product_asin AND end_date = '2999-12-31'::date;
	 
	-- Insert the updated record as the current record
	insert into data_warehouse.product_dim (asin, brand, title, description, im_url, price, start_date, end_date)
	select 		distinct product_asin, product_brand,product_title, product_description, product_im_url, product_price,
				current_date, '2999-12-31'::date
	from 		cte_product_dim;

	
	
END;
$$ LANGUAGE plpgsql;