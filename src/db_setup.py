import psycopg2
import os
import yaml
import pdb

#NOTE: assumes cabinet and cabinet_test databases exist

#DEFINE BLOB_TYPES HERE
custom_blob_types = {
    'youtube':{'photo_id':'VARCHAR', 'channel':'VARCHAR', 'title':'VARCHAR'}
    }

# Connection fns
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

# STANDARD CABINET TABLES
def create_blob_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS blob(
        blob_id VARCHAR(64) PRIMARY KEY,
        blob_b64s TEXT)""") 


def create_demo_table(cur):
    cur.execute("""CREATE TABLE IF NOT EXISTS fruit(
        entry_id SERIAL PRIMARY KEY,
        blob_type VARCHAR,
        fruit_name VARCHAR,
        fruit_color VARCHAR,
        blob_id VARCHAR(64),
        FOREIGN KEY (blob_id) REFERENCES blob(blob_id))""")

def create_new_blob_type(blob_type:str, fields:dict, cur):
    fields_str = ""
    for key in fields:
        field = key+" "+fields[key]+',\n'
        fields_str += field
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {blob_type}(
        entry_id SERIAL PRIMARY KEY,
        blob_type VARCHAR,
        {fields_str}
        blob_id VARCHAR(64),
        FOREIGN KEY (blob_id) REFERENCES blob(blob_id))""")



# CREATE TABLES
def initialize(env):
    try:
        conn, cur = setup_db_connect(env,True) 
        create_blob_table(cur)
        if env == 'testing':
            create_demo_table(cur)
        else: # instantiate your custom blob_types HERE
            for b_type in custom_blob_types:
                create_new_blob_type(b_type, custom_blob_types[b_type], cur)
                print(f"{b_type} blob_type table created")
        print(f'{env} tables created') 
    finally:
        cur.close()
        conn.close() 

if __name__ == '__main__':
    initialize('dev_local')
    initialize('testing') 




