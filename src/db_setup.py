import os
import pdb
import yaml

import psycopg2

#NOTE: assumes databases for each environment already exist and that config/config.yaml contains a connection string to each database

# DEFINE BLOB_TYPES HERE
blob_types = {
    'fruit': {'fruit_name':'VARCHAR', 'fruit_color':'VARCHAR'}, 
    'youtube':{'photo_id':'VARCHAR', 'channel':'VARCHAR', 'category':'VARCHAR', 'title':'VARCHAR'},
    'lichess': {'event':'VARCHAR', 'site':'VARCHAR', 'white':'VARCHAR', 'black':'VARCHAR', 'result':'VARCHAR', 'utcdate':'VARCHAR', 'utctime':'VARCHAR', 'whiteelo':'INT', 'blackelo':'INT', 'whiteratingdiff':'VARCHAR', 'blackratingdiff':'VARCHAR', 'eco':'VARCHAR', 'opening':'VARCHAR', 'timecontrol':'VARCHAR', 'termination':'VARCHAR', }
}

# ASSIGN BLOB_TYPES TO ENVIRONMENTS
env_blobs = {
    'dev_local': ['youtube', 'lichess'],
    'testing': ['fruit', 'youtube']
    }

# Connection fns
def setup_get_conn_str(env:str, config_file:str='config/config.yaml') ->str:
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

# Table creating fns 
def create_blob_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS blob(
        blob_hash VARCHAR(64),
        blob_path VARCHAR PRIMARY KEY,
        status VARCHAR)""") 

def create_new_blob_type(blob_type:str, fields:dict, cur):
    fields_str = ""
    for key in fields:
        field = key+" "+fields[key]+',\n'
        fields_str += field
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {blob_type}(
        entry_id SERIAL PRIMARY KEY,
        blob_type VARCHAR,
        blob_hash VARCHAR(64),
        {fields_str} """)


# Create Tables
def initialize(env):
    try:
        conn, cur = setup_db_connect(env,True) 
        create_blob_table(cur)
        for b_type in env_blobs[env]:
            create_new_blob_type(b_type, blob_types[b_type],cur)
            print(f"{b_type} blob_type table created")
        print(f'{env} tables created') 
    finally:
        cur.close()
        conn.close() 

if __name__ == '__main__':
    initialize('dev_local')
    initialize('testing') 




