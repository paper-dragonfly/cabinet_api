import psycopg2
import yaml

def get_conn_str(env:str='dev_local', config_file:str='config/config.yaml') ->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]['conn_str']
    return conn_str

def db_connect(env:str='dev_local', autocommit:bool=True):
    conn_str = get_conn_str(env)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    conn.autocommit = autocommit
    return conn, cur 

def add_blob(blob_bytes:str, env:str='dev_local'):
    try:
        conn, cur = db_connect(env)
        cur.execute('INSERT INTO blob (bytes) VALUES (%s)', (f'{blob_bytes}',))
        cur.execute('SELECT MAX(blob_id) FROM blob')
        blob_id = cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()
        return blob_id 