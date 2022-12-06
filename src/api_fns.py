from collections import UserString
import os
import pdb
from hashlib import sha256
from typing import List
from collections import defaultdict

import psycopg2
import yaml

from src.classes import Blob_Type, blob_classes


ENV = os.getenv('ENV')


def get_conn_str(env: str=ENV, config_file: str='config/config.yaml') -> str:
    """
    Get string from config file to connect to postgreSQL database
    """
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]['conn_str']
    return conn_str

def db_connect(env:str=ENV, autocommit:bool=True) ->tuple:
    """
    Connect to postgreSQL db, return a connection and cursor object
    """
    conn_str = get_conn_str(env)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    conn.autocommit = autocommit
    return conn, cur 

def get_env_info(env:str=ENV, config_file:str='config/config.yaml')->str:
    with open(f'{config_file}', 'r') as f:
        config_dict = yaml.safe_load(f)
    env_host = config_dict[env]['host']
    env_port = config_dict[env]['API port']
    return env_host, env_port


def generate_paths(blob_hash:str) -> list:
    bucket = ['blobs/']
    return [b+blob_hash for b in bucket]
    

def add_blob_paths(blob_hash:str, paths:List[str], cur) -> bool:
    """
    Stores shaa256 hash for blobs alongside path to where blob is saved. The same blob may be saved in multiple places thus the same hash may appear in multiple entries pointing to different locations
    """
    # is blob already in cabiney? 
    cur.execute("SELECT COUNT(1) FROM blob WHERE blob_hash = %s", (blob_hash,))
    if cur.fetchone()[0]:
        return False 
    # add paths 
    for path in paths:
        cur.execute('INSERT INTO blob VALUES (%s,%s,%s)', (blob_hash,path,'pending')) 
    return True


def build_insert_query(metadata:blob_classes) -> tuple:
    metadata_dict = metadata.dict() 
    # generate str of column names 
    if 'entry_id' in metadata_dict.keys():
        del metadata_dict['entry_id']
    # create string of column headings seperated by comma
    col_str = ", ".join(metadata_dict.keys())
    # generate tuple of entry metadata_values
    entry_vals = tuple(metadata_dict.values())
    # generate %s str of correct length
    s = ("%s,"*len(entry_vals))[0:-1]
    # create query
    query = f'INSERT INTO {metadata.blob_type}({col_str}) VALUES({s}) RETURNING entry_id'
    return query, entry_vals


def add_entry(metadata:blob_classes, cur)->int:
    sql_query, entry_vals = build_insert_query(metadata)
    cur.execute(sql_query, entry_vals)
    entry_id = cur.fetchone()[0] 
    return entry_id


def update_save_status(path:str, cur):
    cur.execute('UPDATE blob SET status = %s WHERE blob_path = %s',('saved', path))
    return True 

#__________________

def all_entries(blob_type: str, cur):
    cur.execute(f"SELECT * FROM {blob_type}")
    matches = cur.fetchall() 
    return build_results_dict(blob_type,matches) 
    

def validate_search_fields(user_search: dict, blob_types: dict=Blob_Type)-> bool:
    #Q: have blob_types as fn arg?
    """
    Confirm blob_type is valid and that all search parameters are attributes of specified blob_type
    """
    # invalid blob_type?
    if not user_search['blob_type'] in blob_types.keys():
        return False 
    blob_type_fields = blob_types[user_search['blob_type']].__fields__.keys() 
    for key in user_search.keys():
        if not key in blob_type_fields:
            return False 
    return True  

    

def build_search_query(blob_type: str, user_search: dict) -> tuple:
    search_conditions = ""
    #Q. this is relying on dict being ordered so search_vals line up with user_search keys...does that matter? solution, turn dict key:vals into tuples (key,val) then make seperate search_vals tuple and loop through k:v_tuple rather than dict keys
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
    columns:list = list(Blob_Type[blob_type].__fields__.keys()) 
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


    

# _______/update________ 

def validate_update_fields(blob_type:str, update_data: dict, blob_types: dict=Blob_Type)-> dict:
    """
    Confirm blob_type is valid and that all update parameters are attributes of specified blob_type
    """
    # invalid blob_type?
    if not blob_type in Blob_Type.keys():
        return {'valid': False, 'error':f'BlobTypeError: {blob_type} is not a valid blob type'} 
    blob_type_fields = Blob_Type[blob_type].__fields__.keys() 
    invalid_fields = []
    for key in update_data.keys():
        if not key in blob_type_fields:
            invalid_fields.append(key)
    if invalid_fields:
        return ({'valid': False, 'error':f'FieldError: invalid fields for blob_type {blob_type} {invalid_fields}'}) 
    return {'valid': True} 


def get_current_metadata(blob_type:str, entry_id:int, cur)-> dict:
    s_sub = '%s'
    cur.execute(f"SELECT * FROM {blob_type} WHERE entry_id = {s_sub}", (entry_id,))
    current_metadata_vals = cur.fetchone()
    col_names:list = list(Blob_Type[blob_type].__fields__.keys())
    current_metadata = {}
    for i in range(len(col_names)):
        current_metadata[col_names[i]] = current_metadata_vals[i]
    return current_metadata


def make_full_update_dict(updates:dict, current_metadata:dict):
    """
    Overwrite current_metadata with updates, remove entry_id key and return in new dict
    """
    full_update = {**current_metadata, **updates}
    del full_update['entry_id']
    return full_update

# ________________

def retrieve_paths(search_args: dict, cur) -> str:
    cur.execute(f"SELECT blob_hash FROM {search_args['blob_type']} WHERE entry_id = %s",(search_args['entry_id'],))
    blob_hash = cur.fetchone()[0] 
    cur.execute('SELECT blob_path From blob WHERE blob_hash=%s',(blob_hash,)) 
    matches = cur.fetchall() #[(p1,),(p2,)]
    paths = []
    for match in matches:
        paths.append(match[0])
    return paths

 