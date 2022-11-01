import pdb
import re

import psycopg2

from src.api_fns import db_connect
import src.api_fns as f
from conftest import clear_tables, clear_all_tables

# db connection fns
def test_get_conn_str():
    """
    GIVEN the get_conn_str fn
    WHEN a development environment is passed to the fn
    THEN assert expected database connection string is returned
    """
    assert len(re.findall('cabinet_test$', f.get_conn_str('testing'))) == 1
    assert len(re.findall('cabinet$', f.get_conn_str('dev_local'))) == 1

def test_db_connect():
    """
    GIVEN the db_connect fn
    WHEN a dev env is passed to fn
    THEN assert a connection and cursor are returned
    """
    try: 
        conn, cur = f.db_connect('testing')
        assert type(conn) == psycopg2.extensions.connection
        assert type(cur) == psycopg2.extensions.cursor
    finally:
        conn.close()
        cur.close()

# /blob fns
def test_add_blob():
    """
    GIVEN a postgres db, the add_blob fn and a blob encoded in bytes
    WHEN blob-bytes are passed to add_blob fn
    THEN assert an integer is returned 
    """
    #create test blob
    s = 'Hello World'
    myblob = s.encode('utf-8')
    # pass to fn
    assert type(f.add_blob(myblob, 'testing')) == int
    clear_tables(['blob'])

# /drawer fns
def test_build_insert_query():
    """"
    GIVEN a dictionary of metadata 
    WHEN dict is passed to fn
    THEN assert expected tuple(sql_query:str, values:tuple) is returned
    """
    metadata = {'a': 'apple', 'b': 'bar'}
    returned = f.build_insert_query("alphabet", metadata)
    expected = (f'INSERT INTO alphabet(a, b) VALUES(%s,%s)',('apple', 'bar'))
    assert returned == expected 


def test_add_entry():
    """
    GIVEN a postgres db and metadata dict (for a stored blob?)
    WHEN dict is passed to add_entry along with the metadata type (table_name)
    THEN assert an int is returned - this should be the entry_id
    """
    try:
        #open connection and populate blob table
        conn, cur = f.db_connect('testing')
        cur.execute('INSERT INTO blob(blob_id) VALUES(1212) ON CONFLICT DO NOTHING')
        #pass metadata to fn 
        metadata = {'photo_id': 'p12', 'title': 'addEntryTest', 'blob_id':'1212'}
        returned_entry_id = f.add_entry('youtube', metadata, cur, 'testing') 
        assert type(returned_entry_id) == int
    finally:
        # clear table, close connections
        cur.execute("DELETE FROM youtube")
        cur.execute("DELETE FROM blob")
        cur.close()
        conn.close()


def test_get_column_names():
    """
    GIVEN a postgres db, the fn and a table name  
    WHEN table name is passed to fn  
    THEN assert returns list of table column names
    """
    try:
        conn, cur =db_connect('testing')
        assert f.get_column_names("youtube",cur,'testing') == ['entry_id','photo_id','channel','title','blob_id']
    finally:
        cur.close()
        conn.close()


def test_get_current_metadata():
    """
    GIVEN a postgres db, get_current_metadata fn and an entry_id
    WHEN entry id is passed fn
    THEN assert returns dict of metadata associated with that id
    """
    try:
        conn, cur = db_connect('testing')
        #populate db with old entry
        cur.execute("DELETE FROM blob")
        cur.execute("INSERT INTO blob(blob_id) VALUES('5')")
        cur.execute("INSERT INTO fruit VALUES('55','banana','green','5')")
        # test fn
        current_metadata = f.get_current_metadata('fruit','55',cur, env='testing')
        assert current_metadata == {'blob_id': 5, 'entry_id': 55, 'fruit_color': 'green', 'fruit_name': 'banana'}
    finally: 
        cur.execute('DELETE FROM fruit')
        cur.execute('DELETE FROM blob')
        cur.close()
        conn.close()
        

def test_make_full_update_dict():
    """
    GIVEN two dictionaries 
    WHEN dicts are passed to make_full_update_dict fn 
    THEN assert returns expected combo dict
    """
    update_dict = {'fruit_color':'yellow'} 
    old_metadata = {'blob_id': 5, 'entry_id': 55, 'fruit_color': 'green', 'fruit_name': 'banana'}
    assert f.make_full_update_dict(update_dict, old_metadata) == {'blob_id': 5, 'fruit_color': 'yellow', 'fruit_name': 'banana'}






# def test_():
#     try:
#         conn, cur = db_connect('testing')
#     finally: 
#         cur.close()
#         conn.close()