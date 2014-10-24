#!/bin/bash

#view and view step3: Import data into HBase.

#Set HBase params
hbase_jar="hbase-server-0.98.4-hadoop2.jar"
hbase_table="T_JL_PSP_SERVICE_VIEW_AND_VIEW"
hbase_columns="HBASE_ROW_KEY,PSP_SERVICE_INFO:ID,PSP_SERVICE_INFO:XCOUNT,PSP_SERVICE_INFO:SERVICE_CODE,PSP_SERVICE_INFO:SERVICE_NAME,PSP_SERVICE_INFO:APPLIED_NUM,PSP_SERVICE_INFO:SERVICE_TYPE1,PSP_SERVICE_INFO:SERVICE_TYPE2,PSP_SERVICE_INFO:RESPONSIBLEMEN_NAME,PSP_SERVICE_INFO:RESPONSIBLEMEN_PHONE,PSP_SERVICE_INFO:RESPONSIBLEMEN_EMAIL,PSP_SERVICE_INFO:SERVICE_PROCESS,PSP_SERVICE_INFO:SERVICE_CHARGE,PSP_SERVICE_INFO:SERVICE_AREA,PSP_SERVICE_INFO:AUDIT_TIME,PSP_SERVICE_INFO:SERVICE_ABILITY,PSP_SERVICE_INFO:ENABLE_PLATFORM,PSP_SERVICE_INFO:ORG_ID,PSP_SERVICE_INFO:ORG_TYPE,PSP_SERVICE_INFO:ORG_NAME,PSP_SERVICE_INFO:AUDIT_STATUS,PSP_SERVICE_INFO:IF_DEL,PSP_SERVICE_INFO:MAIN_SERVICE_MODE,PSP_SERVICE_INFO:SERVICE_TE,PSP_SERVICE_INFO:SERVICE_IMG1,PSP_SERVICE_INFO:SERVICE_IMG2,PSP_SERVICE_INFO:SERVICE_IMG3,PSP_SERVICE_INFO:SERVICE_IMG1_NAME,PSP_SERVICE_INFO:SERVICE_IMG2_NAME,PSP_SERVICE_INFO:SERVICE_IMG3_NAME,PSP_SERVICE_INFO:OWN_WIN,PSP_SERVICE_INFO:TYPE,PSP_SERVICE_INFO:PRICE_START,PSP_SERVICE_INFO:PRICE_END,PSP_SERVICE_INFO:SERVICE_AREA1,PSP_SERVICE_INFO:SERVICE_AREA2,PSP_SERVICE_INFO:SPECIFIC_PRICES,PSP_SERVICE_INFO:SERVICE_URL"

#bulk load params
source_data_path="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/ViewAndView/JlpspServiceVavFullProperty"
hfile_path="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/ViewAndView/JlpspServiceVavHFile"
#注意：bulkload会自动创建${hfile_path}，无需事先创建，该脚本的最后需要删除该目录，以便下次顺利执行

#---------------------------------------------------------

#Generate HFiles
hadoop jar $HBASE_HOME/lib/${hbase_jar} importtsv -Dimporttsv.columns=${hbase_columns}  -Dimporttsv.bulk.output=${hfile_path} ${hbase_table} ${source_data_path}

#---------------------------------------------------------
#Import into HBase
hadoop jar $HBASE_HOME/lib/${hbase_jar} completebulkload ${hfile_path} ${hbase_table}

#---------------------------------------------------------
hadoop fs -rmr ${hfile_path}
