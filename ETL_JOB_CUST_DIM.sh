#!/bin/bash
#Description: Run ETL process 
#  
# Usage:  
#
#Change History:
#  Date        Author         Description
#  ----------  -------------- ------------------------------------
#  2019-02-20  Prwatech       Project SCD 2
#################################################################################
source /usr/tmp/Parameters.txt
export ETLDATA_SCHEMA
export SQOOPJOB
export HIVESCRIPT

#################################################################################
# RUN LOAD PROCESS WITH DEPENDENCY
#################################################################################

echo $ETLDATA_SCHEMA
echo $SQOOPJOB
echo $HIVESCRIPT


QUEUE='dev'

SCRIPTSTARTTIME=$(date +%s)

##This to delete Stage Files for every Run so that only new and updated Records can be populated in next sqoop job

echo "Deleting Stage Files Not Directory (1st Layer)..."
if hadoop fs -test -d /bigdata_project_batch1; then
        hadoop fs -rm /bigdata_project_batch1/*
        echo "HDFS Stage Files in /bigdata_project_batch1/ has been removed."
else
        echo "HDFS directory /bigdata_project_batch1/ is absent."
        exit 0
fi

## This is a Saved Sqoop Job which is required to be created prior to executing this script.
## It will Prompt for a password which will be a mysql password

echo "~~~Starting Sqoop Jobs...~~~"

SQOOPSTARTTIME=$(date +%s)
sqoop job --exec $SQOOPJOB;
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
hive -hiveconf mapred.job.queue.name=$QUEUE -hiveconf ETLDATA_SCHEMA=$ETLDATA_SCHEMA -f $HIVESCRIPT
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


