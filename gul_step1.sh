#!/bin/bash

#guess u like step1: Generate user preference from action log and business.(can we accept invalid action?)
#user preference includes:userid,itemid,preference
#preference is the count of view/buy group by user and item

#---------------------------------------------------------
#Set hive params
hive_database="JILIN_SMEPSP_REC"

#Set business table params
#TMP_VALID_BUSINESS_TABLE是在“看了又看”的step1计算出来的，使用有效业务条件过滤，并进行列减枝，仅保留id和item_category
tmp_valid_business_table="TMP_VALID_BUSINESS_TABLE"

#Set action table params
action_table="T_PSP_RAW_ACTION_LOG"
#该脚本抽取的仅仅是服务相关行为偏好，所以需要设置item_type，如果做交叉推荐则无需设置item_type
item_type_condition="and b.item_type='202000'"

#Set user preference params
#T_USER_PREFERENCE表包含三列：user_id，item_id，preference
user_preference_table="T_USER_PREFERENCE"
view_weight="1"
buy_weight="5"


#---------------------------------------------------------
$HIVE_HOME/bin/hive -e "

use ${hive_database};

insert overwrite table ${user_preference_table}
select c.user_id,c.item_id,count(weight) preference
from
(
select /*+mapjoin(a)*/  
	b.user_id,b.item_id,CASE WHEN '10' THEN ${view_weight} WHEN '20' THEN ${buy_weight} weight
from 
        ${tmp_valid_business_table} a
	join 
	${action_table} b
	on 
	(b.item_id = a.id 
	${item_type_condition})
) c
group by c.user_id,c.item_id
;"
