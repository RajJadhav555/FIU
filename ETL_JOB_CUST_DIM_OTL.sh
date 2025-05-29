#!/bin/bash
#Description: Run ETL process 
#  
# Usage:  
#
#Change History:
#  Date        Author         Description
#  ----------  -------------- ------------------------------------
#  2019-02-20  Prwatech       Project SCD 2 One time load
#################################################################################
source /usr/tmp/Parameters.txt
export ETLDATA_SCHEMA
export HIVESCRIPT_OTL
export MYSQLSCHEMA
export MYSQLTABLE
export HIVE_TABLE


#################################################################################
# RUN LOAD PROCESS WITH DEPENDENCY
#################################################################################

echo $ETLDATA_SCHEMA
echo $HIVESCRIPT_OTL
echo $MYSQLSCHEMA
echo $MYSQLTABLE
echo $HIVE_TABLE

QUEUE='dev'

SCRIPTSTARTTIME=$(date +%s)


## This is a Normal Sqoop Job which will Pull all the records from the rdbms table to /user/hive/warehouse directory as a table.
## This will automatically creates a table in default schema

echo "~~~Starting Sqoop Jobs...~~~"

SQOOPSTARTTIME=$(date +%s)
sqoop import --connect jdbc:mysql://192.168.74.1/$MYSQLSCHEMA --username root --password root --table $MYSQLTABLE --hive-import --hive-table $HIVE_TABLE -m -1;
rc=$?
SQOOPENDTIME=$(date +%s)

echo "It takes $(($SQOOPSTARTTIME - $SQOOPENDTIME)) seconds to complete this task in Sqoop..."

if [ $rc -eq 0 ]; then
            echo "Sqoop Job has been finished successfully"
        else 
            echo "Sqoop Job failed"
            exit $rc
fi


echo "~~~Starting Hive Jobs...~~~"

HIVESTARTTIME=$(date +%s)
hive -hiveconf mapred.job.queue.name=$QUEUE -hiveconf ETLDATA_SCHEMA=$ETLDATA_SCHEMA -hiveconf HIVE_TABLE=$HIVE_TABLE -f $HIVESCRIPT_OTL
rc=$?
HIVEENDTIME=$(date +%s)
echo "It takes $(($HIVEENDTIME - $HIVESTARTTIME)) seconds to complete this task in Hive..."


if [ $rc -eq 0 ]; then
            echo "Hive Script has been successfully executed"
        else 
            echo "hive Script failed"
            exit $rc
fi


SCRIPTENDTIME=$(date +%s)
echo "It takes $(($SCRIPTENDTIME - $SCRIPTSTARTTIME)) seconds to complete all tasks..."

exit 0


