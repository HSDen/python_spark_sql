__author__ = 'tws626'
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import yaml
import subprocess
from pylog import Pylog
from read_s3_parquet import Read_S3_Parquet



class PL_Mapper():

    def __init__(self):
        global log
        global read_s3_parquet
        log = Pylog().log_formatter()
        read_s3_parquet=Read_S3_Parquet()

    #########################################################
    # Method: SQL table
    # Function: Create SQL temporary table and
    #           display sample records
    #Sample Select query to validate SQL execution.
    #########################################################

    def pl_display_records(self):
        card_evt_path= read_s3_parquet.conig_keys("config.yml", "par_hdfs_s3", "s3_file_dir")
        log.info("***************Spark session Initialization**********************")
        spark_session= SparkSession.builder.appName("display_records").getOrCreate()
        df_card_evt= spark_session.read.load("%s"%card_evt_path)
        df_card_evt.registerTempTable("card_evt_type")
        df_card_evt_results= spark_session.sql("select SYS_EVT_TYPE_ID from card_evt_type")
        (df_card_evt_results.show())
        log.info("***************First query completedn**********************")

        df_card_sys_evt= spark_session.read.load("%s"%card_evt_path)
        df_card_sys_evt.registerTempTable("card_sys_evt")
        df_card_sys_evt= spark_session.sql("select SYS_EVT_TYPE_ID from card_evt_type")
        print(df_card_sys_evt.show())
        log.info("***************Second query completedn**********************")

if __name__=='__main__':
    pl_mapper=PL_Mapper()
    pl_mapper.pl_display_records()