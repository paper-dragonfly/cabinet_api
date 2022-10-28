import psycopg2
import yaml

def get_conn_str(env:str='dev_local', config_file:str='config/config.yaml') ->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]['conn_str']
    return conn_str

def db_connect(env:str='dev_local', autocommit:bool=True) ->tuple:
    conn_str = get_conn_str(env)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    conn.autocommit = autocommit
    return conn, cur 

def get_env_host(env:str='dev_local', config_file:str='config/config.yaml')->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    env_host = config_dict[env]['host']
    return env_host


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


def generate_sql_to_insert_metadata(table, metadata:dict):
    # generate str of column names
    columns:list = metadata.keys()
    col_str = str
    for i in columns:
        col_str += i + ', '
    l = len(col_str) 
    col_str = col_str[0:-2]
    # generate str of entry values
    entry_vals = tuple(metadata.values())
    s = ("%s,"*len(entry_vals))[0:-1]
    # subs = str(tuple(entry_vals.values()))[1:-1]
    return col_str, s, entry_vals


def add_entry(table, metadata:dict, env:str='dev_local')->int:
    col_str, s, entry_vals = generate_sql_to_insert_metadata(table, metadata)
    try:
        conn, cur = db_connect(env)
        cur.execute(f'INSERT INTO {table}({col_str}) VALUES({s})', entry_vals)
        cur.execute(f'SELECT MAX (entry_id) FROM {table}') 
        entry_id = cur.fetchone()[0] 
    finally:
        cur.close()
        conn.close()
        return entry_id


 