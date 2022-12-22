import pdb
import re
import base64
from hashlib import sha256
import pytest 

import psycopg2

from src.api_fns import db_connect
import src.api_fns as f
from tests.conftest import clear_tables, clear_all_tables, Session
from src.classes import Fruit, StorageFnSchema
from src.constants import NEW_BLOB, NEW_LOCATION, DUPLICATE
from src.database import BlobTable, FruitTable

# session = Session()

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

    # def test_db_connect(self):
    #     """
    #     GIVEN the db_connect fn
    #     WHEN a dev env is passed to fn
    #     THEN assert a connection and cursor are returned
    #     """
    #     try: 
    #         conn, cur = f.db_connect('testing')
    #         assert type(conn) == psycopg2.extensions.connection
    #         assert type(cur) == psycopg2.extensions.cursor
    #     finally:
    #         conn.close()
    #         cur.close()


class TestInsert:
    def test_check_for_duplicate(self):
        """
        GIVEN a StorageFnSchema instance (metadata, storage_providers)
        WHEN instance is passed to fn 
        ASSERT determins if blob already in cabinet and if blob is duplicate
        """
        clear_all_tables()
        with Session() as session:
            # new blob 
            inst = StorageFnSchema(metadata={'blob_type':'fruit', 'blob_hash':'myhash'}, storage_envs=['testing'])
            assert f.check_for_duplicate(inst, session) == NEW_BLOB
            # duplicate
            d_blob = BlobTable(blob_hash='myhash', blob_path = 'blobs/test/fruit/myhash')
            session.add(d_blob) 
            session.commit()
            assert f.check_for_duplicate(inst, session) == DUPLICATE
            # old blob, new location 
            inst2 = StorageFnSchema(metadata={'blob_type':'fruit', 'blob_hash':'myhash'}, storage_envs=['dev'])
            assert f.check_for_duplicate(inst2, session) == NEW_LOCATION        
            

    def test_generate_paths(self):
        """
        GIVEN a StorageFnSchema instance (metadata, storage_providers)
        WHEN instance is passed to fn
        ASSERT returns expected list of paths
        """
        blob = 'A poem about pineapples'
        blob_hash = sha256(blob.encode('ascii')).hexdigest()
        storage_envs = ['testing', 'dev']
        new_blob_unsaved = StorageFnSchema(metadata={'blob_type':'fruit', 'blob_hash':blob_hash}, storage_envs=storage_envs)
        paths = f.generate_paths(new_blob_unsaved)
        assert paths == {'blobs/test/fruit/fc278761b5697780831b090e61405942acf974f102824e58bcf025fda3f1e357', 'blobs/fruit/fc278761b5697780831b090e61405942acf974f102824e58bcf025fda3f1e357', 'gs://cabinet22_fruit/fc278761b5697780831b090e61405942acf974f102824e58bcf025fda3f1e357'}
        
        with pytest.raises(KeyError):
            StorageFnSchema(metadata={'blob_hash':blob_hash, 'color':'red'}, storage_envs=['testing'])
    
    
    def test_add_blob_paths(self):
        clear_all_tables()
        """
        GIVEN a postgres db, a blob_hash and a list of save_paths
        WHEN blob_hash and save_paths are passed to add_blob_paths fn
        THEN assert returns True
        """
        clear_all_tables()
        #declare test blob_hash
        blob_hash = 'a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e'
        paths = ['cabinet_api/blobs/a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e']
        # pass to fn
        with Session() as session: 
            resp = f.add_blob_paths(blob_hash,paths,session)
            assert resp == True
            resp = f.add_blob_paths(blob_hash, paths, session)
            assert resp == False 


    def test_add_entry(self):
        """
        GIVEN a postgres db and a metadata_pydantic_inst
        WHEN inst is passed to add_entry 
        THEN assert an int is returned - this should be the entry_id
        """
        clear_all_tables()
        #pass metadata to fn 
        metadata = Fruit(entry_id=None, blob_type='fruit', fruit_name= 'strawberry', fruit_color='red', blob_hash='test_blob_hash')
        with Session() as session:
            returned_entry_id = f.add_entry(metadata, session) 
            assert type(returned_entry_id) == int
        


class TestSearch():
    clear_all_tables()
    BLOB_TYPES = {'fruit':Fruit}
    valid1 = {'blob_type':'fruit','fruit_name':'mango'}
    invalidtype = {'blob_type':'house','fruit_name':'banana'}
    invalidkey = {'blob_type':'fruit','fruit_age':'two'}

    def test_validate_search_fields(self):
        """
        GIVEN fn
        WHEN user submits blob_type and dict of metadata search params 
        THEN assert returns expected bool depending on whether or not dict keys match blob_type fields
        """
        assert f.validate_search_fields({'blob_type':'fruit'}) == True
        assert f.validate_search_fields(self.valid1) == True
        assert f.validate_search_fields(self.invalidtype)==False
        assert f.validate_search_fields(self.invalidkey) == False


    def test_build_results_dict(self):
        mango = FruitTable(entry_id=22, blob_type = 'fruit', fruit_name='mango', fruit_color='orange',blob_hash = 'mangohash')
        kiwi = FruitTable(entry_id = 23, blob_type = 'fruit', fruit_name='kiwi', fruit_color='green',blob_hash = 'kiwihash')
        matches = [mango, kiwi]
        resp = f.build_results_dict('fruit', matches) 
        assert resp == {'entry_id':[22,23], 'blob_type':['fruit','fruit'],'fruit_name':['mango','kiwi'], 'fruit_color':['orange','green'], 'blob_hash':['mangohash','kiwihash']}
       

    def test_all_entries(self):
        clear_all_tables()
        # add entries to db
        mango = FruitTable(entry_id=22, blob_type = 'fruit', fruit_name='mango', fruit_color='orange',blob_hash = 'mangohash')
        kiwi = FruitTable(entry_id = 23, blob_type = 'fruit', fruit_name='kiwi', fruit_color='green',blob_hash = 'kiwihash')
        with Session() as session:
            session.add(mango)
            session.add(kiwi)
            session.commit()

        ##COME BACK - WHY DOESN'T SESSION CLOSE AFTER WITH BLOCK???? 

        # use all_entries to retrieve matches 
        resp = f.all_entries('fruit', session)
        # assert returns expected
        assert resp == {'entry_id':[22,23], 'blob_type':['fruit','fruit'],'fruit_name':['mango','kiwi'], 'fruit_color':['orange','green'], 'blob_hash':['mangohash','kiwihash']} 


    def test_search_metadata(self):
        clear_all_tables() 
        # populate db
        mango = FruitTable(entry_id=22, blob_type = 'fruit', fruit_name='mango', fruit_color='orange',blob_hash = 'mangohash')
        kiwi = FruitTable(entry_id = 23, blob_type = 'fruit', fruit_name='kiwi', fruit_color='green',blob_hash = 'kiwihash')
        with Session() as session:
            session.add(mango)
            session.add(kiwi)
            session.commit()
        # assert expected
        assert f.search_metadata('fruit', self.valid1, session) == {'entry_id':[22], 'blob_type':['fruit'],'fruit_name':['mango'], 'fruit_color':['orange'], 'blob_hash':['mangohash']}
        assert f.search_metadata('fruit', {'blob_type':'fruit'}, session) == {'entry_id':[22,23], 'blob_type':['fruit','fruit'],'fruit_name':['mango','kiwi'], 'fruit_color':['orange','green'], 'blob_hash':['mangohash','kiwihash']} 
        assert f.search_metadata('fruit', {'fruit_name':'pear'},session) ==  {}

            
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
        clear_all_tables()
        mango = FruitTable(entry_id=55, blob_type = 'fruit', fruit_name='mango', fruit_color='orange',blob_hash = 'mangohash')
        with Session() as session:
            session.add(mango)
            session.commit() 
        current_metadata = f.get_current_metadata('fruit', 55, session)
        assert current_metadata == {'blob_hash': 'mangohash', 'entry_id': 55, 'blob_type':'fruit', 'fruit_color': 'orange', 'fruit_name': 'mango'}
       
        

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
    pineapple_blob = BlobTable(blob_hash = 'hash1', blob_path= 'f/pineapple')
    pineapple_md = FruitTable(entry_id=101, blob_type = 'fruit', fruit_name='pineapple', fruit_color='orange',blob_hash = 'hash1')
    with Session() as session:
        session.add(pineapple_blob)
        session.add(pineapple_md) 
        session.commit() 
    search_dict = {'blob_type':'fruit','entry_id':101}
    assert f.retrieve_paths(search_dict, session) == ['f/pineapple']


def test_clean():
    clear_all_tables()
    return True 


