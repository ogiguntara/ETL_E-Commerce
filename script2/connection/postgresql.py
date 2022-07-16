from sqlalchemy import create_engine
from psycopg2 import connect

class PostgreSQL:
  def __init__(self, cfg):
      self.host = cfg['host']
      self.port = cfg['port']
      self.username = cfg['username']
      self.password = cfg['password']
      self.database = cfg['database']

  def connect(self, conn_type='engine'):
    #enginer builder
    if conn_type == 'engine':
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
        conn_engine = engine.connect()
        # print("Connect Engine Postgresql \n")
        return engine, conn_engine
    
    #url and properties builder for spark
    elif conn_type == 'spark':
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        driver = 'org.postgresql.Driver'
        properties = {"user": self.username,"password": self.password,"driver": driver}
        return url, properties
    
    else:
        conn = connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
            )
        cursor = conn.cursor()
        print("Connect Cursor Postgresql")
        return conn, cursor