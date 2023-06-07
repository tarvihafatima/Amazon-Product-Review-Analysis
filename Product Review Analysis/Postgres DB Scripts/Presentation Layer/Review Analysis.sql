-- Analysis by Review


-- Reviews with Most Interactions from Users
drop view if exists data_warehouse.most_interactions;
create  view data_warehouse.most_interactions as
with review_interactions as
(
	select review_id, reviewer_id, product_id, review_interactions
	from data_warehouse.product_reviews_fact
)
,

review_interactions_rn as
(  
	select *,
	ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_interactions DESC) AS rn
	from review_interactions
)

select review_id, reviewer_id, product_id, review_interactions
from review_interactions_rn
where rn = 1
order by review_interactions desc
limit 10;


-- Reviews with Most Helpful Votes from Users
drop view if exists data_warehouse.most_most_helpful;
create  view data_warehouse.most_most_helpful as
with review_interactions as
(
	select review_id, reviewer_id, product_id, review_up_votes, review_interactions,
			review_up_votes/ review_interactions  as positive_votes,
			review_up_votes/ review_interactions  * 100 as positive_votes_percentage
	from data_warehouse.product_reviews_fact
	where review_interactions != 0
)
,

review_interactions_rn as
(  
	select *,
	ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY positive_votes DESC) AS rn
	from review_interactions
)

select review_id, reviewer_id, product_id, review_up_votes, review_interactions, positive_votes_percentage
from review_interactions_rn
where rn = 1
order by positive_votes desc, review_interactions desc
limit 10;






