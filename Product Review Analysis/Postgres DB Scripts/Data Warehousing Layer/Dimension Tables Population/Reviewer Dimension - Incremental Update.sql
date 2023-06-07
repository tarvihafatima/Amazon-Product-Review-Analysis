CREATE OR REPLACE FUNCTION incremental_update_reviewer_dimension(input_date DATE)
RETURNS VOID AS $$
begin
	
	-- Populate Reviewer Dimension
	
	drop table if exists cte_reviewer_dim;
	create temporary table cte_reviewer_dim as
	select		distinct reviewer_id as reviewer_id1,
		        reviewer_name as reviewer_name1
	from 		staging_db.reviews
	where       cast(insertion_time as date) >= input_date 
		        and reviewer_id is not null and reviewer_name is not null;

    
	delete from cte_reviewer_dim
	using data_warehouse.reviewer_dim rd
	where rd.reviewer_uuid = reviewer_id1
	and rd.reviewer_name =reviewer_name1;
	
	
	insert into 	data_warehouse.reviewer_dim (reviewer_uuid, reviewer_name)
	select 			reviewer_id1,reviewer_name1
	from 			cte_reviewer_dim
	on conflict 	(reviewer_uuid) 	DO UPDATE 
	set 			reviewer_name =excluded.reviewer_name;
	
END;
$$ LANGUAGE plpgsql;
