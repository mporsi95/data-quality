import os
import warnings
import pandas as pd
import findspark

from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType

findspark.init()

class DhzLib():
    def __init__(self) -> None:
        self.spark = self.create_spark_session()
        self.date_ = date

############################################
# Class initialization functions
############################################
        
    def create_spark_session(self) -> SparkSession:
        print('Creating Spark session')
        try:
            spark =  SparkSession.builder \
                .master('local[*]') \
                .appName('dhzlib') \
                .config('spark.executor.memory', '12g') \
                .config('spark.driver.memory', '12g') \
                .getOrCreate()
            spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
            print(f'SparkSession created at {spark.sparkContext._jsc.sc().uiWebUrl().get()}')
            return spark
        except Exception as e:
            print(e)

############################################
# Data Quality Functions
############################################

    def check_interval_integrity(self,
                           table_name: str,
                           date_column: str,
                           lower_bound: datetime,
                           upper_bound: datetime) -> None:
        try:
            print(f'Checking interval integrity for column {date_column}')
            print(f'Interval: {lower_bound.strftime("%Y-%m-%d")} to {upper_bound.strftime("%Y-%m-%d")}')

            df = self.read_mysql(table_name)
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
        
    def generate_stats_table(self, tables: dict) -> None:
        try:
            stats_df = []
            for table, column in tables.items():
                df = self.read_mysql(table)
                group_df = df.groupBy(F.from_utc_timestamp(
                                F.col(column).cast('timestamp'), 'America/Sao_Paulo') \
                                    .cast('date').alias('date_')
                            ).agg(F.count(column).alias('rows'))
                
                stats = group_df.select(
                    F.min('date_').alias('min_date'),
                    F.max('date_').alias('max_date'),
                    F.sum('rows').alias('total_rows'),
                    F.avg('rows').alias('avg_rows_per_day'),
                    F.stddev('rows').alias('stddev_rows_per_day'),
                    F.min('rows').alias('min_rows_per_day'),
                    F.max('rows').alias('max_rows_per_day'),
                    F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo').alias('stats_date')
                )
                stats = stats.withColumn('table', F.lit(table))
                stats = stats.withColumn('date_column', F.lit(column))
                stats = stats.select(
                    F.col('table'),
                    F.col('date_column'),
                    F.col('min_date'),
                    F.col('max_date'),
                    F.col('total_rows'),
                    F.col('avg_rows_per_day'),
                    F.col('stddev_rows_per_day'),
                    F.col('min_rows_per_day'),
                    F.col('max_rows_per_day'),
                    F.col('stats_date')
                )

                if not stats_df:
                    stats_df = stats
                else:
                    stats_df = stats_df.unionAll(stats)

            self.write_mysql_utils(stats_df, 'tr_data_quality_stats', 'overwrite')
            return
        except Exception as e:
            print(e)
        
    # def check_null_values(self, df: DataFrame) -> None:
    #     try:
    #         nulls = df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns]).collect()
    #         print('Nulls check')
    #         print(nulls)
    #         return
    #     except Exception as e:
    #         print(e)
    #         return


    # def check_duplicates(self, df: DataFrame) -> None:
    #     try:
    #         duplicates = df.groupBy(df.columns).count().filter('count > 1').collect()
    #         print('Duplicates check')
    #         print(duplicates)
    #         return
    #     except Exception as e:
    #         print(e)
   


############################################
# Database Helper Functions
############################################

    def test_connection_mysql(self) -> None:
        try:
            properties = {
                'user': os.getenv('MYSQL_USER'),
                'password': os.getenv('MYSQL_PASSWORD')
            }
            test_connection = self.spark.read.jdbc(os.getenv('MYSQL_URL_UTILS'), 'teste_conexao', properties=properties)

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

            print('Data written in MySQL')
            return
        except Exception as e:
            print(e)

    def write_mysql_utils(self, df: DataFrame, table: str, mode_: str) -> None:
        try:
            properties = {
                'user': os.getenv('MYSQL_USER'),
                'password': os.getenv('MYSQL_PASSWORD')
            }
            df.write.jdbc(os.getenv('MYSQL_URL_UTILS'), table, properties=properties, mode=mode_)

            print('Data written in MySQL')
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
    
    