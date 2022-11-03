import psycopg2
import os
import yaml
import pdb

#NOTE: assumes cabinet and cabinet_test databases exist

def setup_get_conn_str(env:str='dev_local', config_file:str='config/config.yaml') ->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]['conn_str']
    return conn_str

def setup_db_connect(env:str, autocommit:bool=False):
    conn_str = setup_get_conn_str(env)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    conn.autocommit = autocommit
    return conn, cur 

def create_blob_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS blob(
        blob_id VARCHAR(64) PRIMARY KEY,
        bytes TEXT)""") 

def create_youtube_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS youtube(
        entry_id SERIAL PRIMARY KEY,
        blob_type VARCHAR, 
        photo_id VARCHAR,
        channel VARCHAR,
        Title VARCHAR, 
        blob_id VARCHAR(64),
        FOREIGN KEY (blob_id) REFERENCES blob(blob_id))""")

def create_fruit_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS fruit(
        entry_id SERIAL PRIMARY KEY,
        blob_type VARCHAR,
        fruit_name VARCHAR,
        fruit_color VARCHAR,
        blob_id VARCHAR(64),
        FOREIGN KEY (blob_id) REFERENCES blob(blob_id))""")

def initialize(env):
    try:
        conn, cur = setup_db_connect(env,True) 
        create_blob_table(cur)
        create_youtube_table(cur) 
        if env == 'testing':
            create_fruit_table(cur)
        print(f'{env} tables created') 
    finally:
        cur.close()
        conn.close() 

if __name__ == '__main__':
    initialize('dev_local')
    initialize('testing') 




