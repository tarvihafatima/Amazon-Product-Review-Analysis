CREATE OR REPLACE FUNCTION data_warehouse.incremental_load_dimensions_and_fact(input_date date)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
	SELECT data_warehouse.incremental_update_product_dimension(input_date);
	SELECT data_warehouse.incremental_update_product_category_dimension(input_date);
	SELECT data_warehouse.incremental_update_price_bucket_dimension(input_date);
	SELECT data_warehouse.incremental_product_category_bridge_dimension(input_date);
	SELECT data_warehouse.incremental_update_sales_rank_dimension(input_date);

	SELECT data_warehouse.incremental_update_also_bought_dimension(input_date);
	SELECT data_warehouse.incremental_update_also_viewed_dimension(input_date);
	SELECT data_warehouse.incremental_update_bought_together_dimension(input_date);
	SELECT data_warehouse.incremental_update_buy_after_view_dimension(input_date);

	SELECT data_warehouse.incremental_update_reviewer_dimension(input_date);
	SELECT data_warehouse.incremental_update_review_dimension(input_date);

	SELECT data_warehouse.incremental_update_product_review_fact(input_date);

END;
$function$
;
