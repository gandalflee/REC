#!/bin/bash

#view and view step1: Generate x and x data from action-log and filter invalid data by join business table.
#Steps：
#1.business_table 使用业务有效过滤条件 =》 tmp_valid_business_table
#2.tmp_valid_business_table + action_table =》 tmp_vs_action_table看服务表
#3.tmp_vs_action_table自连接 =》 x_and_x_table

#Set hive params
hive_database="JILIN_SMEPSP_REC"

#Set action table params
action_table="T_PSP_RAW_ACTION_LOG"
action_type="10"
item_type="202000"
user_id="rcookie_id"
#这里是“服务看了又看”，所以只需要基于原始日志ETL出一张“看服务”的临时表，并在生成x_and_x_table时用该表做自连接
#如果是“看了又买”就还需要ETL出一张“买服务”的表，在生成x_and_x_table时，用“看服务”和“买服务”的表做连接
tmp_vs_action_table="TMP_VS_ACTION_TABLE"

#Set business table params
business_table="T_PSP_BUSINESS_SERVICE"
valid_conditions="if_del='1'"
item_category="service_type1"
tmp_valid_business_table="TMP_VALID_BUSINESS_TABLE"

#Set x and x table params
x_and_x_table="T_PSP_SERVICE_VIEW_AND_VIEW"

#---------------------------------------------------------
$HIVE_HOME/bin/hive -e "

use ${hive_database};

insert overwrite table ${tmp_valid_business_table}
select id,${item_category} item_category
from ${business_table}
where ${valid_conditions}
;

insert overwrite table ${tmp_vs_action_table}
select /*+mapjoin(a)*/  
	b.${user_id} user_id,b.${user_id} rcookie_id,'NULL' login_status,b.item_id item_id,a.item_category item_category,'${action_type}' action_type,'${item_type}' item_type
from 
        ${tmp_valid_business_table} a
	join 
	${action_table} b
	on 
	(b.item_id = a.id and 
	b.action_type = '${action_type}' and 
	b.item_type = '${item_type}')
limit 10000
;

insert overwrite table ${x_and_x_table}
select c.current_item_id current_item_id,
       c.current_item_category current_item_category,
       c.andx_item_id andx_item_id,
       count(*) xcount,
       0 xrank
  from (select a.item_id      current_item_id,
               a.item_category current_item_category,
               b.item_id      andx_item_id

          from ${tmp_vs_action_table} a join
               ${tmp_vs_action_table} b
	       on
               a.${user_id} = b.${user_id}
	  where a.item_id != b.item_id
	 ) c
 group by c.current_item_id, c.current_item_category, c.andx_item_id
;"
