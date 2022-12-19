from collections import UserString
import os
import pdb
from hashlib import sha256
from typing import List
from collections import defaultdict

import psycopg2
import yaml
from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import exists 

from src.constants import BLOB_TYPES, blob_classes, NEW_BLOB, NEW_LOCATION, DUPLICATE
from src.classes import StorageFnSchema
from src.database import BlobTable, YoutubeTable, FruitTable, TABLE_BLOB_TYPE_MATCHING


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

def check_for_duplicate(storage_fn_inst: StorageFnSchema, session) -> str:
    """ 
    Checks if blob is already in Cabinet. If yes, is user trying to save to new location(s)?
    """
    blob_hash = storage_fn_inst.metadata['blob_hash']
    matching_blobs = session.query(BlobTable).filter_by(blob_hash=blob_hash).all()
    # is blob already in cabinet?
    if not matching_blobs:
        return NEW_BLOB
    # is user requesting to save in a location where blob is already saved (i.e. duplicate)? 
    saved_paths = [entry.blob_path for entry in matching_blobs]
    with open(f'config/config.yaml', 'r') as f:
        config_dict = yaml.safe_load(f)
    storage_options: dict = config_dict['storage_providers'][storage_fn_inst.metadata['blob_type']]
    for env in storage_fn_inst.storage_envs: 
        for path in storage_options[env]:
            if path+'/'+blob_hash in saved_paths:
                return DUPLICATE
    # blob in cabinet but user requesting to save to new location 
    return NEW_LOCATION

def generate_paths(new_blob_unsaved: StorageFnSchema) -> set:
    blob_type = new_blob_unsaved.metadata['blob_type']
    blob_hash = new_blob_unsaved.metadata['blob_hash']
    storage_envs: list = new_blob_unsaved.storage_envs 
    with open(f'config/config.yaml', 'r') as f:
        config_dict = yaml.safe_load(f)
    storage_options: dict = config_dict['storage_providers'][blob_type] 
    paths = set()
    for env in storage_envs:
        for path in storage_options[env]:
            path = path + "/" + blob_hash 
            paths.add(path)
    return paths
    

def add_blob_paths(blob_hash:str, paths:List[str], cur) -> bool:
    """
    Stores sha256 hash for blobs alongside path to where blob is saved. The same blob may be saved in multiple places thus the same hash may appear in multiple entries pointing to different locations
    """
    try:
        for path in paths:
            cur.execute('INSERT INTO blob VALUES (%s,%s)', (blob_hash,path)) 
    except Exception:
        return False 
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


def add_entry(parsed_metadata_inst:blob_classes, cur)->int:
    sql_query, entry_vals = build_insert_query(parsed_metadata_inst)
    cur.execute(sql_query, entry_vals)
    entry_id = cur.fetchone()[0] 
    return entry_id

def get_entry_id(blob_type: str, blob_hash: str, cur):
    cur.execute("SELECT entry_id FROM ")


def update_save_status(path:str, cur):
    cur.execute('UPDATE blob SET status = %s WHERE blob_path = %s',('saved', path))
    return True 

#__________________

def validate_search_fields(user_search: dict, blob_types: dict=BLOB_TYPES)-> bool:
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


def build_results_dict(blob_type:str, matches:List[blob_classes]) -> dict:
    """
    create dict with blob_type metadata column names as keys and list of values from matching entries as values
    """
    results = defaultdict(list)
    for match in matches:
        for key, val in match.__dict__.items():
            if key in BLOB_TYPES[blob_type].__fields__.keys(): 
                results[key].append(val)  
    return results


def all_entries(blob_type: str, session) -> dict: #**
    matches = [match for match in session.query(TABLE_BLOB_TYPE_MATCHING[blob_type]).all()]
    return build_results_dict(blob_type, matches) 


def search_metadata(blob_type: str, user_search: dict, session)-> dict:
    """
    Get all entries matching user's serach params
    """
    resp = session.query(TABLE_BLOB_TYPE_MATCHING[blob_type])
    for key,val in user_search.items():
        resp = resp.filter_by(**{key:val})
    results_dict = build_results_dict(blob_type, resp.all())
    return results_dict 
        
    # query, search_vals = build_search_query(blob_type, user_search)
    # cur.execute(query, search_vals)
    # matches = cur.fetchall() 
    # if not matches:
    #     return None
    # results_dict = build_results_dict(blob_type, matches)
    # return results_dict 


    

# _______/update________ 

def validate_update_fields(blob_type:str, update_data: dict, blob_types: dict=BLOB_TYPES)-> dict:
    """
    Confirm blob_type is valid and that all update parameters are attributes of specified blob_type
    """
    # invalid blob_type?
    if not blob_type in BLOB_TYPES.keys():
        return {'valid': False, 'error':f'BlobTypeError: {blob_type} is not a valid blob type'} 
    blob_type_fields = BLOB_TYPES[blob_type].__fields__.keys() 
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
    col_names:list = list(BLOB_TYPES[blob_type].__fields__.keys())
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

 