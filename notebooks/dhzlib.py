import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

class dhzlib():
    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("dhzlib").getOrCreate()
        self.load_env()
    
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
                'password': os.getenv('MYSQL_PASSWORD')
            }
            df = self.spark.read.jdbc(os.getenv('MYSQL_URL'), table, properties=properties)
            print(f'Data read from MySQL')
            return df
        except Exception as e:
            print(e)