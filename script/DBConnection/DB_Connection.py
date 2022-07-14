import mysql.connector
import sys
import os
import warnings

sys.path.append('.\config')
import config

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

warnings.filterwarnings('ignore')

try:
    conn =  mysql.connector.connect(
        host=config.DB_CONNECTION_INFO['ENDPOINT'], 
        user=config.DB_CONNECTION_INFO['USER'], 
        passwd=config.DB_CONNECTION_INFO['USER'], 
        port=config.DB_CONNECTION_INFO['PORT'], 
        database=config.DB_CONNECTION_INFO['DBNAME']
    )
    
    cur = conn.cursor()
    cur.execute("CREATE TABLE test (Seq int, Name VARCHAR(100), PRIMARY KEY(Seq));")
    conn.commit()
    conn.close()
except Exception as e:
    print("Database connection failed due to {}".format(e))
