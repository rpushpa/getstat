#! /bin/bash

if [ $#  -ne 3 ]; then
     echo "illegal number of parameters.Please provide three prameters 1.directory name 2.urlto download the filename3 HDFS location"
    exit 1;
fi

echo "********************************************************************************************************************"
echo "First arg : directory to land the file: $1"
echo "Second arg:url to download the file $2"
echo "third arg:HDFS location $3"
echo "downloadling the file..."
echo "*******************************************************************************************************************"
DIR=$1
HDFS_DIR=$3


# wget log file
LOGFILE=wget_`date +"%Y_%m_%d_%H_%M"`.log

# wget download url
URL=$2

cd $DIR
wget  -O getstat_com_serp_report_`date +"%Y%m"`.csv.gz  $URL  -o $LOGFILE

if [ $?  -ne 0 ]; then
     echo "Download failed.pls check the log $LOGFILE "
    exit 2;
fi

echo "********************************************************************************************************************"
echo "download successful...."

echo "unzipiing the file...."
echo "********************************************************************************************************************"

gunzip $DIR/getstat_com_serp_report_`date +"%Y%m"`.csv.gz
#gunzip $DIR/getstat_com_serp_report_201707.csv.gz

if [ $?  -ne 0 ]; then
   echo "unzippping  the file failed."
   exit 3;
fi

echo "********************************************************************************************************************"

echo "gunziping is successful...."

echo "Coping file on to HDFS...."

echo "********************************************************************************************************************"

hadoop fs -copyFromLocal $DIR/getstat_com_serp_report_`date +"%Y%m"`.csv  $HDFS_DIR
# hadoop fs -copyFromLocal $DIR/getstat_com_serp_report_201707.csv  $HDFS_DIR/

if [ $?  -ne 0 ]; then
   echo "Copy to hdfs failed."
   exit 4;
fi

echo "********************************************************************************************************************"

echo "Copy to hdfs is sucessful...."

echo "creating stage table from downloaded file..."

echo "********************************************************************************************************************"

hive -hiveconf file_path=$HDFS_DIR/getstat_com_serp_report_`date +"%Y%m"`.csv hive.cli.errors.ignore=true  -f stg_keyword_rank.hql


if [ $?  -ne 0 ]; then
   echo "stage table creating failed. "
   exit 5;
fi

echo "********************************************************************************************************************"
 
echo "stage table is created..."

echo "Creating partioned table...."

echo "********************************************************************************************************************"


hive hive.cli.errors.ignore=true -f partitioned_kw_rank.hql

if [ $?  -ne 0 ]; then
   echo "partioned table creation failed. "
   exit 6;
fi

echo "********************************************************************************************************************"

echo "partition table and kw_rank1_ptn is created  successfully...."
echo "two final tables to be created for analysing keywords on convergence..."


echo "********************************************************************************************************************"

hive hive.cli.errors.ignore=true -f getstat_final_steps.hql

if [ $?  -ne 0 ]; then
   echo "creating analysis tables failed. "
   exit 7;
fi

echo "********************************************************************************************************************"

echo "two final tables are created sucessfully.now you can run final SQLs from steps to execute to get the results....."

echo "All parts of script got  executed successfully...."


echo "********************************************************************************************************************"



exit 0
