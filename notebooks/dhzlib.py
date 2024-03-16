import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

class dhzlib():
    def __init__(self) -> None:
        self.spark = self.create_spark_session()
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