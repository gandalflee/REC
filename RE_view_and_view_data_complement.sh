#!/bin/bash

$HIVE_HOME/bin/hive -e "
insert overwrite table test_x_and_x_full
select * from
(select rank() over(partition by d.current_item_category,d.current_item_id order by d.source,d.view_count desc) no,
d.current_item_id, d.current_item_category, d.andx_item_id, d.source, d.view_count
from
(select current_item_id,current_item_category,andx_item_id,1 source,view_count
 from test_x_and_x
union all
select c.* from
(select a.current_item_id current_item_id,a.current_item_category current_item_category,b.item_id andx_item_id,2 source,b.view_count view_count
 from
(select current_item_id,current_item_category from test_x_and_x
  group by current_item_id,current_item_category) a, test_topn b
 where a.current_item_category = b.item_category) c
 left outer join test_x_and_x d on (c.current_item_id=d.current_item_id
  and c.current_item_category=d.current_item_category AND c.andx_item_id=d.andx_item_id)
where d.andx_item_id is null
 ) d
) e where e.no <= 5;
"
