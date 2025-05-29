--------------------------------------------------------------------------
--Prwatech SCD TYPE  2 From MYSQL 
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

--This temporary table will be used to hold intermediate result

create temporary table ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp as 
select * from ${hiveconf:ETLDATA_SCHEMA}.customerdim_main limit 0;



--Step 1: Load expired records from MAIN to temp table
INSERT INTO ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp
SELECT * FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_main
WHERE ACTIVE_FLAG = 'N' and EFF_TO_DT is not null;


--Step 2: Get all records which are going to expire
INSERT into ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp
SELECT TGT.CustomerID ,
TGT.CustomerAltID,
TGT.CustomerName,
TGT.Gender,
TGT.Loaddate,
TGT.EFF_FR_DT,
date_sub(current_date(),1) as EFF_TO_DT,
'N' as ACTIVE_FLAG,
TGT.updatedate
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_main TGT
join ${hiveconf:ETLDATA_SCHEMA}.customerdim_stage1 SRC 
on TGT.CustomerID = src.CustomerID
and TGT.ACTIVE_FLAG = 'Y'
where hash(SRC.CustomerAltID,SRC.CustomerName,SRC.Gender) <> 
hash(TGT.CustomerAltID,TGT.CustomerName,TGT.Gender);



--Step 3: Copy active records from main to temp table
INSERT INTO ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp
SELECT 
TGT.CustomerID ,
TGT.CustomerAltID,
TGT.CustomerName,
TGT.Gender,
TGT.Loaddate,
TGT.EFF_FR_DT,
TGT.EFF_TO_DT,
TGT.ACTIVE_FLAG,
TGT.updatedate
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_main TGT
WHERE TGT.ACTIVE_FLAG = 'Y'
AND NOT EXISTS (SELECT 1 FROM
${hiveconf:ETLDATA_SCHEMA}.customerdim_stage1 SRC
WHERE TGT.CustomerID = src.CustomerID )

UNION ALL 

SELECT 
TGT.CustomerID ,
TGT.CustomerAltID,
TGT.CustomerName,
TGT.Gender,
TGT.Loaddate,
TGT.EFF_FR_DT,
TGT.EFF_TO_DT,
TGT.ACTIVE_FLAG,
TGT.updatedate
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_main TGT
join ${hiveconf:ETLDATA_SCHEMA}.customerdim_stage1 SRC
on TGT.CustomerID = src.CustomerID
and TGT.ACTIVE_FLAG = 'Y'
where hash(SRC.CustomerAltID,SRC.CustomerName,SRC.Gender) =
hash(TGT.CustomerAltID,TGT.CustomerName,TGT.Gender);



--Step 4: Copy only updated records from Stage table
INSERT INTO ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp
SELECT DISTINCT
src.CustomerID ,
src.CustomerAltID,
src.CustomerName,
src.Gender,
src.Loaddate,
current_date() AS EFF_FR_DT,
null AS EFF_TO_DT,
'Y' AS ACTIVE_FLAG,
cast(cast(src.loaddate as timestamp) as date) AS updatedate
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_stage1 src
join ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp INT1 
on src.CustomerID = INT1.CustomerID
AND INT1.ACTIVE_FLAG = 'N'
left outer join ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp INT2
on INT1.CustomerID = INT2.CustomerID
and INT2.ACTIVE_FLAG = 'Y'
where INT2.CustomerID is NULL;



--Step 5: Copy fresh records from Stage to Temp

INSERT INTO ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp
SELECT 
src.CustomerID ,
src.CustomerAltID,
src.CustomerName,
src.Gender,
src.Loaddate,
current_date() AS EFF_FR_DT,
null AS EFF_TO_DT,
'Y' AS ACTIVE_FLAG,
cast(cast(src.loaddate as timestamp) as date) AS updatedate
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_stage1 SRC
WHERE NOT EXISTS     
(SELECT 1
FROM   ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp temp
WHERE  SRC.CustomerID = temp.CustomerID);


--Finally Replace the content of the main table in a transactional manner:
INSERT OVERWRITE TABLE ${hiveconf:ETLDATA_SCHEMA}.customerdim_main PARTITION(updatedate)
SELECT *
FROM ${hiveconf:ETLDATA_SCHEMA}.customerdim_temp;

