-- c.3.i.
use db_consumer_panel;
-- select CTL_BR products
drop table if exists ctl_br;
create temporary table ctl_br
select department_at_prod_id, count(*) as ctl_number from products  where brand_at_prod_id='CTL BR' group by department_at_prod_id;
select * from ctl_br;

-- We define 'more Private Labelled' as: 
-- In one certain category, "the number of private labelled products/ the number of whole products in that category " is higher
-- So we calculate that portion of every category and compare them in bar graph.
select department_at_prod_id, ctl_number/total_number as percentage_share from
(select department_at_prod_id, total_number, ctl_number from ctl_br
inner join (select department_at_prod_id, count(*) as total_number from products group by department_at_prod_id) as t
using (department_at_prod_id))as a order by percentage_share;
