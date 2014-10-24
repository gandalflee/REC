#!/bin/bash

#view and view step2: Data completion using topn of same category.
#Steps：
#1.x_and_x_table + topn_table =》 topn_completion_table
#2.topn_completion_table 去掉和 x_and_x_table 重复的数据
#2.x_and_x_table + 去重后的topn_completion_table =》 x_and_x_completion_table

#Set hive params
hive_database="JILIN_SMEPSP_REC"

#Set x and x table params
x_and_x_table="T_PSP_SERVICE_VIEW_AND_VIEW"
x_and_x_completion_table="T_PSP_SERVICE_COMPLETION_VIEW_AND_VIEW"
rec_amount="25"

#Set topn table params
topn_table="T_PSP_SERVICE_VIEW_TOPN"
topn_completion_table="TMP_SVAV_COMPLETION_BY_TOPN"

#---------------------------------------------------------
$HIVE_HOME/bin/hive -e "
use ${hive_database};


insert overwrite table ${topn_completion_table}
  select /*+mapjoin(a)*/
   b.current_item_id       current_item_id,
   b.current_item_category current_item_category,
   a.item_id               andx_item_id,
   2                       source,
   a.xcount
    from (select item_id, item_category, count(*) xcount
            from ${topn_table}
           group by item_id, item_category) a
    join (select current_item_id, current_item_category
            from ${x_and_x_table}
           group by current_item_id, current_item_category) b
      on b.current_item_category = a.item_category;


insert overwrite table ${x_and_x_completion_table}
select f.*
  from (select e.current_item_id,
               e.current_item_category,
               e.andx_item_id,
               e.xcount,
	       row_number() over(partition by e.current_item_id order by e.source asc, e.xcount desc) xrank
          from (

		select current_item_id,
                       current_item_category,
                       andx_item_id,
                       1 source,
                       xcount
                  from ${x_and_x_table}

                union all

                select c.*
                  from ${topn_completion_table} c
                  left outer join ${x_and_x_table} d
                    on (c.current_item_id = d.current_item_id and
                       c.current_item_category = d.current_item_category and
                       c.andx_item_id = d.andx_item_id)
                 where d.andx_item_id is null
		 
		 ) e

		 ) f
 where f.xrank <= ${rec_amount}
;"
