import os
import warnings

from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

class DhzLib():
    def __init__(self) -> None:
        self.spark = self.create_spark_session()
        self.date_ = date
        self.F_ = F
        self.create_temp_tables()

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

            df = self.spark.read.table(table_name)
            days_between = (upper_bound - lower_bound).days + 1
            filtered_df = df.filter(F.col(date_column).cast('date').between(lower_bound, upper_bound))
            distinct_days = filtered_df.select(F.col(date_column).cast('date')).distinct().count()
            
            if distinct_days != days_between:
                expected_dates = {(lower_bound + timedelta(days=index)) for index in range(days_between)}
                actual_dates = filtered_df.select(F.col(date_column).cast('date')).distinct().toPandas()[date_column].tolist()
                dates_missing = expected_dates.symmetric_difference(actual_dates)

                warnings.warn(f'''
        Integrity check failed for {date_column}.
        Expected {days_between} days, but got {distinct_days} days.
        Dates missing: {sorted([date.strftime("%Y-%m-%d") for date in dates_missing], reverse=True)}
                        ''')
                return
            
            print(f'Integrity check passed for {date_column}')
            return
        except Exception as e:
            print(e)
            return
        
    def check_if_updated(self, table: str, date_column: str, limit_date: datetime.date, schema: str = None):
        """
        Função que verifica se a tabela está atualizada
        """
        limit_date_str = limit_date.strftime('%Y-%m-%d')
        table_path = f"{schema}.{table}" if schema else table
        num_rows = self.spark.read.table(table_path).where(f"{date_column} > '{limit_date_str}'").count()
        if num_rows > 0:
            print(f"A tabela {table_path} está atualizada. Linhas após data mencionada: {num_rows}.")
        else:
            print(f"A tabela {table_path} está desatualizada")

    def check_if_valid(self, table: str, valid_values: dict, schema: str = None):
        check_functions = {
            'string': self.check_if_valid_string
        }
        results = []
        table_path = f"{schema}.{table}" if schema else table
        df = self.spark.read.table(table_path)
        table_dtypes = df.dtypes
        for column in valid_values.keys():
            column_dtype = [dtype for col, dtype in table_dtypes if col == column]
            column_dtype = column_dtype[0] if column_dtype else None

            if not column_dtype:
                print(f"Column {column} not found in table {table_path}")
                continue

            if column_dtype not in check_functions.keys():
                raise ValueError(f"Data type {column_dtype} not supported")
            
            status, invalid_values = check_functions[column_dtype](column, valid_values[column], df)
            results.append([column, status, invalid_values])
        headers = ["Nome da Coluna", "Status", "Outliers"]        
        format_row = "{:>15}" * (len(headers))
        print(format_row.format(*headers))
        for column, status, invalid_values in results:
            print(format_row.format(column, status, invalid_values))
        return None
            
    def check_if_valid_string(self, column: str, valid_values: list, df: DataFrame):
        invalid_values = df.filter(~df[column].isin(valid_values)).count()
        if invalid_values > 0:
            return 'ERROR', invalid_values
        return 'OK', invalid_values
        
    def generate_stats_table(self, tables: dict) -> None:
        try:
            stats_df = []
            for table, column in tables.items():
                df = self.spark.read.table(table)
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

            stats_df.createOrReplaceTempView('tr_data_quality_stats')
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
    
############################################
# TEMPORARY
############################################

    def create_temp_tables(self) -> None:
        files = ['../data/202401-capitalbikeshare-tripdata.csv', '../data/202402-capitalbikeshare-tripdata.csv']
        raw_df = self.spark.read.csv(files, header=True, inferSchema=True)
        raw_df.createOrReplaceTempView('raw_capital_bikeshare')
    
        tr_df = raw_df.select(
        raw_df.ride_id.alias('ride_id'),
        raw_df.rideable_type.alias('rideable_type'),
        F.from_utc_timestamp(raw_df.started_at.cast('timestamp'), 'America/Sao_Paulo').alias('started_at'),
        F.from_utc_timestamp(raw_df.ended_at.cast('timestamp'), 'America/Sao_Paulo').alias('ended_at'),
        raw_df.start_station_name.alias('start_station_name'),
        raw_df.start_station_id.cast('int').alias('start_station_id'),
        raw_df.end_station_name.alias('end_station_name'),
        raw_df.end_station_id.cast('int').alias('end_station_id'),
        raw_df.start_lat.cast('float').alias('start_lat'),
        raw_df.start_lng.cast('float').alias('start_lng'),
        raw_df.end_lat.cast('float').alias('end_lat'),
        raw_df.end_lng.cast('float').alias('end_lng'),
        raw_df.member_casual.alias('member_casual')
        )

        tr_df.createOrReplaceTempView('tr_capital_bikeshare')
        print('Temporary tables created')
        return