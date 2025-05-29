--------------------------------------------------------------------------
--Prwatech SCD TYPE  2 One time load From MYSQL 
--
--
-- Revision History
--
-- Date        Author           Description
-- ----------  ------------     ------------------------
-- 2019-02-27  Prwatech         original creation
---------------------------------------------------------------------------- 


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;


use ${hiveconf:ETLDATA_SCHEMA};

insert overwrite table ${hiveconf:ETLDATA_SCHEMA}.customerdim_main PARTITION(updatedate) 
select 
CustomerID ,
CustomerAltID ,
CustomerName ,
Gender ,
cast(Loaddate as timestamp),
current_date(),
null,
'Y',
cast(cast(Loaddate as timestamp) as date) 
from 
default.${hiveconf:HIVE_TABLE};



