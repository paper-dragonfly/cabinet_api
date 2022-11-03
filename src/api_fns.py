from collections import UserString
import os
import pdb
from hashlib import sha256
from typing import List
from collections import defaultdict

import psycopg2
import yaml

from src.classes import blob_types


ENV = os.getenv('ENV')


def get_conn_str(env: str=ENV, config_file: str='config/config.yaml') -> str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]['conn_str']
    return conn_str

def db_connect(env:str=ENV, autocommit:bool=True) ->tuple:
    conn_str = get_conn_str(env)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    conn.autocommit = autocommit
    return conn, cur 

def get_env_host(env:str=ENV, config_file:str='config/config.yaml')->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    env_host = config_dict[env]['host']
    return env_host


def add_blob(blob_b64s:str, cur) -> str: 
    # TODO: add shaw256 hash as blob_id
    #convert blob_b64s -> blob_bytes
    blob_b64_bytes = blob_b64s.encode('ascii')
    blob_id = sha256(blob_b64_bytes).hexdigest()
    cur.execute('INSERT INTO blob VALUES (%s,%s)', (blob_id,f'{blob_b64s}',))
    return blob_id


def build_insert_query(table:str, metadata:dict) -> tuple:
    # generate str of column names 
    del metadata['entry_id']
    col_str = ", ".join(metadata.keys())
    # generate tuple of entry metadata_values
    entry_vals = tuple(metadata.values())
    # generate %s str of correct length
    s = ("%s,"*len(entry_vals))[0:-1]
    # create query
    query = f'INSERT INTO {table}({col_str}) VALUES({s}) RETURNING entry_id'
    return query, entry_vals


def add_entry(table:str, metadata:dict, cur)->int:
    sql_query, entry_vals = build_insert_query(table, metadata)
    cur.execute(sql_query, entry_vals)
    entry_id = cur.fetchone()[0] 
    return entry_id
#__________________

def all_entries(blob_type: str, cur):
    cur.execute(f"SELECT * FROM {blob_type}")
    matches = cur.fetchall() 
    return build_results_dict(blob_type,matches) 
    

def validate_search_fields(blob_type: str, user_search: dict, blob_types: dict=blob_types)-> bool:
    #Q: have blob_types as fn arg?
    """
    Confirm blob_type is valid and that all search parameters are attributes of specified blob_type
    """
    if not blob_type in blob_types.keys():
        return False 
    # valid blob_type but no search args 
    if not user_search:
        return True 
    blob_type_fields = blob_types[blob_type].__fields__.keys() 
    for key in user_search.keys():
        if not key in blob_type_fields:
            return False 
    return True   
    

def build_search_query(blob_type: str, user_search: dict) -> tuple:
    search_conditions = ""
    search_vals = tuple(user_search.values())
    for key in user_search:
        search_conditions += f"{key}= %s AND "
    search_conditions = search_conditions[0:-5]
    query = f"SELECT * FROM {blob_type} WHERE {search_conditions}"
    return query, search_vals 

def build_results_dict(blob_type, matches:List[tuple]) -> dict:
    """
    create dict with blob_type metadata column names as keys and list of values from matching entries as values
    """
    results = defaultdict(list)
    columns:list = list(blob_types[blob_type].__fields__.keys()) 
    for m in matches:
        for i in range(len(m)):
            results[columns[i]].append(m[i])    
    return results

def search_metadata(blob_type: str, user_search: dict, cur)-> dict:
    query, search_vals = build_search_query(blob_type, user_search)
    cur.execute(query, search_vals)
    matches = cur.fetchall() 
    if not matches:
        return None
    results_dict = build_results_dict(blob_type, matches)
    return results_dict 


    

# _______________ 
def get_current_metadata(table:str, entry_id:int, cur, env:str=ENV)-> dict:
    s_sub = '%s'
    cur.execute(f"SELECT * FROM {table} WHERE entry_id = {s_sub}", (entry_id,))
    old_metadata_vals: list= list(cur.fetchone())
    col_names:list = get_column_names(table, cur)
    old_metadata = {}
    for i in range(len(col_names)):
        old_metadata[f'{col_names[i]}'] = old_metadata_vals[i]
    return old_metadata


def make_full_update_dict(updates:dict, old_metadata:dict):
    full_update = updates 
    newdict = {**old, **new}
    full_update.pop('entry_id', 'No entry_id key') #or use del ?
    return full_update

 