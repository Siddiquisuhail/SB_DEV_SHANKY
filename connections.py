from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from pyathena import connect
from dotenv import load_dotenv
import os


load_dotenv()


class Connections:
    """ 
    This Class is used to generate connections to the database. Just simply import Connections Module and use context Manager Like "With".
    
    The available database connections are:
    
    1) ADB - adb_connect()
    2) UserDB - user_db_connect()
    3) CricketDB - cricket_db_connect()
    4) Athena - athena_connect()
    """
    def __init__(self):
        self.drivername = os.getenv('DRIVERNAME')
        self.username = os.getenv('USERNAME')
        self.password = os.getenv('PASSWORD')


    @contextmanager
    def adb_connect(self) -> Engine:
        self.host = os.getenv('HOST_ADB')
        self.port = os.getenv('PORT_ADB')
        self.database = os.getenv('DATABASE_ADB')
        engine = create_engine(f"{self.drivername}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}", echo_pool=True)
        print(f"Connecting with engine {engine}")
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            print("Connection closed")
            
    
    @contextmanager
    def user_db_connect(self) -> Engine:
        self.host = os.getenv('HOST_USERDB')
        self.port = os.getenv('PORT_USERDB')
        self.database = os.getenv('DATABASE_USERDB')
        engine = create_engine(f"{self.drivername}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}", echo_pool=True)
        print(f"Connecting with engine {engine}")
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            print("Connection closed")

    @contextmanager
    def cricket_db_connect(self) -> Engine:
        self.host = os.getenv('HOST_CRICKETDB')
        self.port = os.getenv('PORT_CRICKETDB')
        self.database = os.getenv('DATATABASE_CRICKETDB')
        engine = create_engine(f"{self.drivername}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}", echo_pool=True)
        print(f"Connecting with engine {engine}")
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            print("Connection closed")
            
    @contextmanager
    def athena_connect(self) -> Engine:
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('REGION_NAME')
        self.s3_staging_dir = os.getenv('S3_STAGING_DIRECTORY')
        
        engine = create_engine(f"awsathena+rest://{self.aws_access_key_id}:{self.aws_secret_access_key}@athena.{self.region_name}.amazonaws.com:443/"f"default?s3_staging_dir={self.s3_staging_dir}",echo_pool=True)
 
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            print("Connection closed")





