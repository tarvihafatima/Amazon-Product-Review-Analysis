-- Product Reviews Consolidated View


drop view if exists data_warehouse.product_reviews_consolidated_view;
create view data_warehouse.product_reviews_consolidated_view as

select 	rd.review_id, rd.review_summary, rd.review_text,
		pd.product_id, pd.asin, pd.brand, pd.title, pd.description, pd.im_url, pd.price,
		pd.start_date as product_record_entry_date, pd.end_date as product_record_expiry_date, 
		case when pd.end_date = '2999-12-31'::date then 1 else 0 end as product_latest_record,
		rrd.reviewer_id, rrd.reviewer_uuid, rrd.reviewer_name,
		pcd.category_id, pcd.category_level, category_name,
		pbd.bucket_id, pbd.bucket_name, pbd.start_range as bucket_start_range, pbd.end_range as bucket_end_range,
		dtd.datetime_id, dtd.unix_time,
		dd.date_id, dd.date_value, dd.day_of_week, dd.day_of_month, dd.day_of_year, dd.month_name, dd.month, dd.quarter, dd.year,
		td.time_id, td.time_value, td.hour, td.minute, td.second,
		srd.sales_rank_id, srd.sales_rank_category, srd.sales_rank, srd.start_date as sales_rank_record_entry_date, 
		srd.end_date as sales_rank_record_expiry_date, 
		case when srd.end_date = '2999-12-31'::date then 1 else 0 end as sales_rank_latest_record
		
		
from 	data_warehouse.product_reviews_fact prf
       
		join data_warehouse.review_dim rd 
		on rd.reviewer_id = prf.reviewer_id
		and rd.product_id = prf.product_id

		join data_warehouse.product_dim  pd
		on pd.product_id = rd.product_id 

		join data_warehouse.product_category_bridge_dim pcbd 
		on pd.product_id = pcbd.product_id
		
		join data_warehouse.reviewer_dim rrd
		on rrd.reviewer_id = rd.reviewer_id
		
		join data_warehouse.datetime_dim dtd
		on dtd.datetime_id = rd.unix_review_time_id
		
		join data_warehouse.date_dim dd
		on dtd.date_id = dd.date_id
		
		join data_warehouse.time_dim td
		on td.time_id = td.time_id
		
		left join data_warehouse.product_category_dim pcd 
		on pcbd.category_id = pcbd.category_id
		
		left join data_warehouse.price_bucket_dim pbd
		on pbd.category_id = pcd.category_id
		and pbd.start_range <= pd.price
		and pbd.end_range >= pd.price
		
		left join data_warehouse.sales_rank_dim srd
		on srd.product_id = pd.product_id
		