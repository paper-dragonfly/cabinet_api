import pdb
import re
import base64

import psycopg2

from src.api_fns import db_connect
import src.api_fns as f
from conftest import clear_tables, clear_all_tables
from src.classes import Fruit 

class TestConnections:
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


class TestInsert:
    def test_add_blob(self):
        """
        GIVEN a postgres db, the add_blob fn and a blob encoded as a base_64_bytes_str
        WHEN blob is passed to add_blob fn
        THEN assert an integer of length 64 is returned (sha256 Hash)
        """
        #create test blob
        s = 'Hello World'
        myblob:bytes = s.encode('ascii')
        blob_b64 = base64.b64encode(myblob)
        blob_b64_str = blob_b64.decode('ascii')
        # pass to fn
        try:
            conn, cur = f.db_connect('testing')
            resp = f.add_blob(blob_b64_str,cur)
            assert type(resp) == str
            assert len(resp) == 64
        finally:
            cur.close()
            conn.close()
            clear_tables(['blob'])


    def test_build_insert_query(self):
        """"
        GIVEN a dictionary of metadata 
        WHEN dict is passed to fn
        THEN assert expected tuple(sql_query:str, values:tuple) is returned
        """
        metadata = {'entry_id':None, 'a': 'apple', 'b': 'bar'}
        returned = f.build_insert_query("alphabet", metadata)
        expected = (f'INSERT INTO alphabet(a, b) VALUES(%s,%s) RETURNING entry_id',('apple', 'bar'))
        assert returned == expected 


    def test_add_entry(self):
        """
        GIVEN a postgres db and metadata dict (for a stored blob?)
        WHEN dict is passed to add_entry along with the metadata type (blob_type)
        THEN assert an int is returned - this should be the entry_id
        """
        try:
            #open connection and populate blob table
            conn, cur = f.db_connect('testing') 
            cur.execute("INSERT INTO blob(blob_id) VALUES('test_blob_hash') ON CONFLICT DO NOTHING")
            #pass metadata to fn 
            metadata = {'entry_id':None,'fruit_name': 'strawberry', 'fruit_color': 'red', 'blob_id':'test_blob_hash'}
            returned_entry_id = f.add_entry('fruit', metadata, cur) 
            assert type(returned_entry_id) == int
        finally:
            # clear table, close connections
            cur.execute("DELETE FROM fruit")
            cur.execute("DELETE FROM blob")
            cur.close()
            conn.close()

class TestSearch():
    clear_all_tables()
    blob_types = {'fruit':Fruit}
    valid1 = {'fruit_name':'banana'}
    invalidkey = {'fruit_age':'two'}

    def test_validate_search_fields(self):
        assert f.validate_search_fields('fruit', {}, self.blob_types) == True
        assert f.validate_search_fields('fruit',self.valid1,self.blob_types) == True
        assert f.validate_search_fields('house',self.valid1,self.blob_types)==False
        assert f.validate_search_fields('fruit',self.invalidkey,self.blob_types) == False

    
    def test_build_search_query(self):
        assert f.build_search_query('fruit',self.valid1) == ("SELECT * FROM fruit WHERE fruit_name= %s",('banana',))


    def test_build_results_dict(self):
        matches = [('1','plum','red','phash'),('2','plum','green','phash')]
        assert f.build_results_dict('fruit',matches) == {'entry_id':['1','2'], 'fruit_name':['plum','plum'], 'fruit_color':['red','green'], 'blob_id':['phash','phash']}

    def test_search_metadata(self):
        try:
            conn, cur = db_connect('testing')
            cur.execute("INSERT INTO blob(blob_id) VALUES('hash1'),('hash2')")
            cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s),(%s,%s,%s,%s)",('1','banana','yellow','hash1','2','mango','yellow','hash2'))
            assert f.search_metadata('fruit',self.valid1,cur) == {'entry_id':[1], 'fruit_name':['banana'], 'fruit_color':['yellow'], 'blob_id':['hash1']}
            assert f.search_metadata('fruit',{'fruit_color':'yellow'},cur) == {'entry_id':[1,2], 'fruit_name':['banana','mango'], 'fruit_color':['yellow','yellow'], 'blob_id':['hash1','hash2']}
            assert f.search_metadata('fruit',{'fruit_name':'pear'},cur) == None 
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