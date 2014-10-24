#!/bin/bash

#view and view step3: Property completion.

#Set hive params
hive_database="JILIN_SMEPSP_REC"

#Set x and x table params
x_and_x_completion_table="T_PSP_SERVICE_COMPLETION_VIEW_AND_VIEW"
x_and_x_full_table="T_PSP_SERVICE_FULL_PROPERTY_VIEW_AND_VIEW"
service_url="www.jilin.com/service.action?id="

#Set business table params
business_table="T_PSP_BUSINESS_SERVICE"

#---------------------------------------------------------
$HIVE_HOME/bin/hive -e "
use ${hive_database};
insert overwrite table ${x_and_x_full_table}
select	/*+mapjoin(a)*/ 
	concat(b.current_item_id,'_top',b.xrank),b.andx_item_id,b.xcount,a.SERVICE_CODE,a.SERVICE_NAME,a.APPLIED_NUM,a.SERVICE_TYPE1,a.SERVICE_TYPE2,a.RESPONSIBLEMEN_NAME,a.RESPONSIBLEMEN_PHONE,a.RESPONSIBLEMEN_EMAIL,a.SERVICE_PROCESS,a.SERVICE_CHARGE,a.SERVICE_AREA,a.AUDIT_TIME,a.SERVICE_ABILITY,a.ENABLE_PLATFORM,a.ORG_ID,a.ORG_TYPE,a.ORG_NAME,a.AUDIT_STATUS,a.IF_DEL,a.MAIN_SERVICE_MODE,a.SERVICE_TE,a.SERVICE_IMG1,a.SERVICE_IMG2,a.SERVICE_IMG3,a.SERVICE_IMG1_NAME,a.SERVICE_IMG2_NAME,a.SERVICE_IMG3_NAME,a.OWN_WIN,a.TYPE,a.PRICE_START,a.PRICE_END,a.SERVICE_AREA1,a.SERVICE_AREA2,a.SPECIFIC_PRICES,concat('${service_url}',b.andx_item_id) SERVICE_URL
from 
	${business_table} a
	join
	${x_and_x_completion_table} b
	on
	a.id = b.current_item_id
;"
