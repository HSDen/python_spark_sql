__author__ = 'tws626'

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Column
import json


class Ratings():
     def __init__(self):
        pass

     def ratings_analysis(self):
        spksession= SparkSession.builder.appName("ratings_analysis").getOrCreate()
        ratings_df = spksession.read.format("CSV").option("header","true").load('ratings.csv')
        sample_df=spksession.read.format("CSV").option("header","true").load('rating_joins.csv')
        ###################################
        # Distinct function
        ###################################
        ratings_df.select("userId", "movieId","timestamp").distinct().show()
         #########################################################################################################
        # filter function
        # Working commands: and, or, entire strings
        #Not working: snow%,
        #########################################################################################################
        ratings_df.filter("movieName ='snowden' and rating='3.5'").\
        select("userId", "movieName","movieId","timestamp").show()
         #########################################################################################################
        # join function
        # Working commands: Only able to sel all columns
        #Not working:Not able to select certain cols
        #########################################################################################################
        join_op_ratings= ratings_df.join(sample_df).where(ratings_df['movieName']==sample_df['movieName'])
        join_op_ratings.show(100,True)

         #########################################################################################################
        # filter to act as JOIN
        # Working commands: Only able to sel all columns
        #Not working:Not able to select certain cols
        #########################################################################################################
        ratings_df.join(sample_df, ratings_df.movieName==sample_df.movieName,'left').\
        select(ratings_df.userId,sample_df.userId,sample_df.movieName).show()

if __name__=='__main__':
    ratings=Ratings()
    ratings.ratings_analysis()






