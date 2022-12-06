import pdb
import re
import base64
from hashlib import sha256

import psycopg2

from src.api_fns import db_connect
import src.api_fns as f
from tests.conftest import clear_tables, clear_all_tables
from src.classes import Fruit 

class TestConnections:
    # db connection fns
    def test_get_conn_str(self):
        """
        GIVEN the get_conn_str fn
        WHEN a development environment is passed to the fn
        THEN assert expected database connection string is returned
        """
        assert len(re.findall('cabinet_test$', f.get_conn_str('testing'))) == 1
        assert len(re.findall('cabinet$', f.get_conn_str('dev_local'))) == 1

    def test_db_connect(self):
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
    def test_generate_paths(self):
        """
        GIVEN a sha256 hash of the blob
        WHEN has is passed to fn
        ASSERT returns expected list of paths
        """
        blob = 'Hello World'
        blob_hash = sha256(blob.encode('ascii')).hexdigest()
        paths = f.generate_paths(blob_hash)
        assert paths == ['blobs/a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e']

    def test_add_blob_paths(self):
        clear_all_tables()
        """
        GIVEN a postgres db, the add_blob fn and a blob encoded as a base_64_bytes_str
        WHEN blob is passed to add_blob fn
        THEN assert an integer of length 64 is returned (sha256 Hash)
        """
        #declare test blob_hash
        blob_hash = 'a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e'
        paths = ['cabinet_api/blobs/a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e']
        # pass to fn
        try:
            conn, cur = f.db_connect('testing')
            resp = f.add_blob_paths(blob_hash,paths,cur)
            assert resp == True
            resp = f.add_blob_paths(blob_hash, paths, cur)
            assert resp == False 
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
        metadata = Fruit(entry_id=55, blob_type='fruit', fruit_name='apple', fruit_color='gold',blob_hash='hash')
        returned = f.build_insert_query(metadata)
        expected = (f'INSERT INTO fruit(blob_type, fruit_name, fruit_color, blob_hash) VALUES(%s,%s,%s,%s) RETURNING entry_id',('fruit', 'apple','gold','hash'))
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
            cur.execute("INSERT INTO blob(blob_hash, blob_path) VALUES('test_blob_hash','folder/test_blob_hash') ON CONFLICT DO NOTHING")
            #pass metadata to fn 
            metadata = {'entry_id':None,'fruit_name': 'strawberry', 'fruit_color': 'red', 'blob_hash':'test_blob_hash'}
            metadata = Fruit(entry_id=None, blob_type='fruit', fruit_name= 'strawberry', fruit_color='red', blob_hash='test_blob_hash')
            returned_entry_id = f.add_entry(metadata, cur) 
            assert type(returned_entry_id) == int
        finally:
            # clear table, close connections
            cur.execute("DELETE FROM fruit")
            cur.execute("DELETE FROM blob")
            cur.close()
            conn.close()

    def test_update_save_status(self):
        clear_all_tables()
        #open connection and populate blob table
        try: 
            conn, cur = f.db_connect('testing') 
            cur.execute("INSERT INTO blob(blob_hash, blob_path, status) VALUES('test_blob_hash','folder/test_blob_hash', 'pending') ON CONFLICT DO NOTHING")
            # update
            resp = f.update_save_status('folder/test_blob_hash', cur)
            assert resp == True 
            cur.execute("SELECT status FROM blob WHERE blob_path = %s",('folder/test_blob_hash',))
            assert cur.fetchone()[0] == 'saved'
        finally:
            # clear table, close connections
            cur.execute("DELETE FROM fruit")
            cur.execute("DELETE FROM blob")
            cur.close()
            conn.close()

class TestSearch():
    clear_all_tables()
    Blob_Type = {'fruit':Fruit}
    valid1 = {'blob_type':'fruit','fruit_name':'banana'}
    invalidtype = {'blob_type':'house','fruit_name':'banana'}
    invalidkey = {'blob_type':'fruit','fruit_age':'two'}

    def test_validate_search_fields(self):
        assert f.validate_search_fields({'blob_type':'fruit'}, self.Blob_Type) == True
        assert f.validate_search_fields(self.valid1,self.Blob_Type) == True
        assert f.validate_search_fields(self.invalidtype,self.Blob_Type)==False
        assert f.validate_search_fields(self.invalidkey,self.Blob_Type) == False

    
    def test_build_search_query(self):
        assert f.build_search_query('fruit',self.valid1) == ("SELECT * FROM fruit WHERE blob_type= %s AND fruit_name= %s",('fruit','banana'))


    def test_build_results_dict(self):
        matches = [('1','fruit','plum','red','phash'),('2','fruit','plum','green','phash')]
        assert f.build_results_dict('fruit',matches) == {'entry_id':['1','2'], 'blob_type':['fruit','fruit'],'fruit_name':['plum','plum'], 'fruit_color':['red','green'], 'blob_hash':['phash','phash']}

    def test_search_metadata(self):
        try:
            conn, cur = db_connect('testing')
            cur.execute("INSERT INTO blob(blob_hash, blob_path) VALUES('hash1','f/h1'),('hash2','f/h2')")
            cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s)",('1','fruit','banana','yellow','hash1','2','fruit','mango','yellow','hash2'))
            assert f.search_metadata('fruit',self.valid1,cur) == {'entry_id':[1], 'blob_type':['fruit'], 'fruit_name':['banana'], 'fruit_color':['yellow'], 'blob_hash':['hash1']}
            assert f.search_metadata('fruit',{'fruit_color':'yellow'},cur) == {'entry_id':[1,2], 'blob_type':['fruit','fruit'],'fruit_name':['banana','mango'], 'fruit_color':['yellow','yellow'], 'blob_hash':['hash1','hash2']}
            assert f.search_metadata('fruit',{'fruit_name':'pear'},cur) == None 
        finally: 
            cur.close()
            conn.close()
            
class TestFnsUpdate:
    clear_all_tables()

    def test_update_fields(self):
        assert f.validate_update_fields('fruit', {'fruit_name':'mango'}) == {'valid':True}
        assert f.validate_update_fields('truck', {'fruit_name':'mango'}) == {'valid':False, 'error':'BlobTypeError: truck is not a valid blob type'}
        assert f.validate_update_fields('fruit', {'fruit_age':'mango'}) == {'valid':False, 'error':f"FieldError: invalid fields for blob_type fruit ['fruit_age']"}


    def test_get_current_metadata(self):
        """
        GIVEN a postgres db, get_current_metadata fn and an entry_id
        WHEN entry id is passed fn
        THEN assert returns dict of metadata associated with that id
        """
        try:
            conn, cur = db_connect('testing')
            #populate db with old entry
            cur.execute("INSERT INTO blob(blob_hash, blob_path) VALUES('hash5','f/h5')")
            cur.execute("INSERT INTO fruit VALUES('55','fruit','banana','green','hash5')")
            # test fn
            current_metadata = f.get_current_metadata('fruit','55',cur)
            assert current_metadata == {'blob_hash': 'hash5', 'entry_id': 55, 'blob_type':'fruit', 'fruit_color': 'green', 'fruit_name': 'banana'}
        finally: 
            cur.execute('DELETE FROM fruit')
            cur.execute('DELETE FROM blob')
            cur.close()
            conn.close()
        

    def test_make_full_update_dict(self):
        """
        GIVEN two dictionaries 
        WHEN dicts are passed to make_full_update_dict fn 
        THEN assert returns expected combo dict
        """
        update_dict = {'fruit_color':'yellow'} 
        old_metadata = {'blob_hash': 'hash5', 'entry_id': 55, 'fruit_color': 'green', 'fruit_name': 'banana'}
        assert f.make_full_update_dict(update_dict, old_metadata) == {'blob_hash': 'hash5', 'fruit_color': 'yellow', 'fruit_name': 'banana'}


def test_retrieve_paths():
    clear_all_tables()
    try:
        # populate db with blob and metadata
        conn, cur = db_connect('testing')
        cur.execute('INSERT INTO blob VALUES(%s, %s, %s)',('hash1','f/pineapple','saved'))
        cur.execute('INSERT INTO fruit(entry_id, fruit_name, blob_hash) VALUES(%s,%s,%s)',(101,'pineapple','hash1'))
        search_dict = {'blob_type':'fruit','entry_id':101}
        assert f.retrieve_paths(search_dict, cur) == ['f/pineapple']
    finally:
        cur.close()
        conn.close()


def test_clean():
    clear_all_tables()
    return True 


