__author__ = 'Hari, Gnanaprakasam'
from pyspark.sql import SQLContext
from pyspark import SparkContext
# from pyspark.sql import SparkSession
import yaml
import subprocess
from pylog import Pylog





class Read_S3_Parquet():


    def __init__(self):
        global log
        log = Pylog().log_formatter()

    #########################################################
    # Method: Set the path of S3 file
    # Function: return S3 file value
    #########################################################

    def conig_keys(self, cfg_file_name, cfg_hash_name, cfg_key_name):
        with open(cfg_file_name, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            for key_json in cfg:
                log.info("***************Key json is " + key_json)
                get_s3_file_val = cfg[cfg_hash_name][cfg_key_name]
                log.info("***************Value of get s3 file value is " + get_s3_file_val)
                return get_s3_file_val

    #########################################################
    # Method: Check if the directory already exists in HDFS
    # Function: delete if file exists and create new dir
    #########################################################

    def check_dir_hdfs(self):
        hdfs_file_path = self.conig_keys("config.yml", "hdfs_local", "hdfs_file_dir")
        if (subprocess.call(["hdfs", "dfs", "-test", "-d", "%s"] %hdfs_file_path) == 0):
            log.info("***************Directory exists in HDFS and will be deleted***************")
            subprocess.call(["hdfs", "dfs", "-rm", "-r","%s"]%hdfs_file_path)
        else:
            log.info("***************Specified directory does not exist***************")

    #########################################################
    # Method: Call s3 file location method
    # Function: Download the files from S3 location under
    # temp dir
    #########################################################

    def get_s3_file(self):
        global sc
        global get_s3_file_val
        self.check_dir_hdfs()
        sc = SparkContext("local", "readfiles")
        hadoopconf = sc._jsc.hadoopConfiguration()
        hadoopconf.set("fs.s3a.enableServerSideEncryption", "true")
        hadoopconf.set("fs.s3a.server-side-encryption-algorithm", "AES256")
        log.info("***************Value of Spark Context is" + sc.version)
        get_s3_file_val = self.conig_keys("config.yml", "s3_get_parquet_loc", "s3_file_one")
        get_file_loc = sc.textFile(get_s3_file_val)
        get_hdfs_dir_path=self.conig_keys("config.yml","hdfs_local","hdfs_file_dir")
        get_file_loc.saveAsTextFile("%s"%get_hdfs_dir_path)


    #########################################################################
    # Method: Create sample dataframe
    # Function: Store the dataframe as parquet in dedicated S3 bucket
    #****sample method for validation purposes
    #########################################################################
    def s3_write_data(self):
        global sc
        global put_s3_file_val
        sc = SparkContext("local", "readfiles")
        hadoopconf = sc._jsc.hadoopConfiguration()
        hadoopconf.set("fs.s3a.enableServerSideEncryption", "true")
        hadoopconf.set("fs.s3a.server-side-encryption-algorithm", "AES256")
        log.info("***************Value of Spark Context is" + sc.version)
        with open("config.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        for key_json in cfg:
            log.info("***************Key json is " + key_json)
            put_s3_file_val = cfg['s3_parquet_location']['s3path']

        data_set = sc.parallelize([(0, 1), (0, 1), (0, 2), (1, 2), (1, 10), (1, 20), (3, 18), (3, 18), (3, 18)])
        sqlcontxt = SQLContext(sc)
        df = sqlcontxt.createDataFrame(data_set, ["id", "score"])
        df.write.parquet(put_s3_file_val)



if __name__=='__main__':
    Read_S3_Parquet.get_s3_file()
