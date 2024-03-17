import os
import warnings
import pandas as pd

from datetime import datetime, date
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

class DhzLib():
    def __init__(self) -> None:
        self.spark = self.create_spark_session()
        self.date_ = date
        self.load_env()
    
    def create_spark_session(self) -> SparkSession:
        print('Creating Spark session')
        try:
            spark =  SparkSession.builder \
                .master('local[*]') \
                .appName('dhzlib') \
                .config('spark.executor.memory', '8g') \
                .config('spark.driver.memory', '8g') \
                .getOrCreate()
            spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
            print(f'SparkSession created at {spark.sparkContext._jsc.sc().uiWebUrl().get()}')
            return spark
        except Exception as e:
            print(e)

    def load_env(self):
        try:
            loaded = load_dotenv(verbose=False)
            if loaded:
                return "Environment variables loaded"
            else:
                raise Exception("Error loading environment variables")
        except Exception as e:
            print(e)

############################################
# Data Quality Functions
############################################

    def check_interval_integrity(self,
                           df: DataFrame,
                           date_column: str,
                           lower_bound: datetime,
                           upper_bound: datetime) -> None:
        try:
            print(f'Checking interval integrity for column {date_column}')
            print(f'Interval: {lower_bound.strftime("%Y-%m-%d")} to {upper_bound.strftime("%Y-%m-%d")}')

            days_between = (upper_bound - lower_bound).days + 1
            filtered_df = df.filter(F.col(date_column).cast('date').between(lower_bound, upper_bound))
            distinct_days = filtered_df.select(F.col(date_column).cast('date')).distinct().count()
            
            if distinct_days != days_between:
                expected_dates = set(pd.date_range(start=lower_bound,
                                                    end=upper_bound,
                                                    freq='d') \
                                        .map(lambda x: datetime.date(x)).tolist())
                
                actual_dates = filtered_df.select(F.col(date_column).cast('date')).distinct().toPandas()[date_column].tolist()
                dates_missing = expected_dates.symmetric_difference(actual_dates)

                warnings.warn(f'''
                            Integrity check failed for {date_column}.
                            Expected {days_between} days, but got {distinct_days} days.
                            Dates missing: {[date.strftime("%Y-%m-%d") for date in dates_missing]}
                        ''')
                return
            
            print(f'Integrity check passed for {date_column}')
            return
        except Exception as e:
            print(e)
            return



    # def CheckNulls(self, df: DataFrame) -> None:
    #     try:
    #         nulls = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).collect()
    #         print('Nulls check')
    #         print(nulls)
    #         return
    #     except Exception as e:
    #         print(e)

    # def CheckDuplicates(self, df: DataFrame) -> None:
    #     try:
    #         duplicates = df.groupBy(df.columns).count().filter('count > 1').collect()
    #         print('Duplicates check')
    #         print(duplicates)
    #         return
    #     except Exception as e:
    #         print(e)

    # def CheckDataQuality(self, df: DataFrame) -> None:
    #     self.CheckNulls(df)
    #     self.CheckDuplicates(df)            


############################################
# Database Helper Functions
############################################

    def test_connection_mysql(self) -> None:
        try:
            properties = {
                'user': os.getenv('MYSQL_USER'),
                'password': os.getenv('MYSQL_PASSWORD')
            }
            test_connection = self.spark.read.jdbc(os.getenv('MYSQL_URL'), 'teste_conexao', properties=properties)
            if not test_connection:
                raise Exception("Error connecting to MySQL")
            print('Connected to MySQL')
            return
        except Exception as e:
            print(e)
    
    def write_mysql(self, df: DataFrame, table: str) -> None:
        try:
            properties = {
                'user': os.getenv('MYSQL_USER'),
                'password': os.getenv('MYSQL_PASSWORD')
            }
            df.write.jdbc(os.getenv('MYSQL_URL'), table, properties=properties)
            print('Data written to MySQL')
            return
        except Exception as e:
            print(e)

    def read_mysql(self, table: str) -> DataFrame:
        try:
            properties = {
                'user': os.getenv('MYSQL_USER'),
                'password': os.getenv('MYSQL_PASSWORD'),
                'driver': 'com.mysql.cj.jdbc.Driver'
            }
            df = self.spark.read.jdbc(os.getenv('MYSQL_URL'), table, properties=properties).persist()
            print(f'Data read from MySQL')
            return df
        except Exception as e:
            print(e)