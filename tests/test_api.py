import json
import pdb
import base64
from urllib import response
from hashlib import sha256

import pytest

from src.api import db_connect
import src.api as api
from tests.conftest import clear_all_tables, Session
from src.constants import NEW_LOCATION, NEW_BLOB, DUPLICATE
from src.database import BlobTable, FruitTable

def _test_fn(client):
    # try connecting to db
    # populate db with test data 
    # send http request and capture response 
    # assert response is as expected
    # clear the db
    # close conn and cur
    pass
     

def test_home(client):
    """
    GIVEN a flask app 
    WHEN a GET request is submitted to /home
    THEN assert returns expected string
    """
    response = client.get('/') 
    assert response.data.decode("ASCII") == 'WELCOME TO CABINET'
    

def test_store_envs(client):
    """
    GIVEN a flask app
    WHEN a GET request is submitted to /store_envs
    THEN assert returns expected lists of hosts
    """
    response = client.get('/store_envs?blob_type=fruit')
    assert json.loads(response.data.decode("ASCII"))["body"]['envs'] == ['testing', 'dev']


def test_storage_urls(client):
    """
    GIVEN a flask app 
    WHEN a POST request with blob metadata and storage_ens is sent to /storage_urls 
    ASSERT returns expected paths 
    """
    clear_all_tables()
    metadata = {'blob_type':'fruit','fruit_name':'passion','fruit_color':'purple', 'blob_hash':'phash'}
    envs = ['testing','dev']
    api_resp = client.post('/storage_urls', data=json.dumps({'metadata':metadata, 'storage_envs':envs}), content_type='application/json')
    assert json.loads(api_resp.data.decode('ascii'))['body']['new'] == NEW_BLOB
    assert set(json.loads(api_resp.data.decode('ascii'))['body']['paths']) == set(['blobs/test/fruit/phash','gs://cabinet22_fruit/phash', 'blobs/fruit/phash'])
    
   

class TestBlob:

    def test_blob_get(self,client):
        """
        GIVEN a flask app 
        WHEN a GET request is sent to /blob containing search parameters as url query args
        THEN assert returns all entries matching search parameters 
        """
        clear_all_tables()
        with Session() as session:
            # populate_db 
            b1 = BlobTable(blob_hash = 'hash1', blob_path='f/h1')
            b2 = BlobTable(blob_hash = 'hash2', blob_path='f/h2')
            b3 = BlobTable(blob_hash = 'hash3', blob_path='f/h3')
            banana = FruitTable(entry_id = 1,blob_type = 'fruit', fruit_name = 'banana', fruit_color= 'yellow',blob_hash= 'hash1')
            apple = FruitTable(entry_id = 2,blob_type = 'fruit', fruit_name = 'apple', fruit_color='red',blob_hash='hash2')
            strawberry = FruitTable(entry_id = 3,blob_type = 'fruit', fruit_name = 'strawberry', fruit_color='red',blob_hash='hash3')
            banana2 = FruitTable(entry_id = 4,blob_type = 'fruit', fruit_name = 'banana', fruit_color= 'green',blob_hash= 'hash1')
            session.add_all([b1,b2,b3, banana, apple, strawberry, banana2])
            session.commit() 
        # submit GET request
        r_no_args = client.get("/blob")
        r_no_type = client.get('/blob?fruit_name=banana')
        r_return_all_entries_of_blobtype = client.get("/blob?blob_type=fruit")
        response1 = client.get("/blob?blob_type=fruit&entry_id=2")
        response2 = client.get('/blob?blob_type=fruit&fruit_color=red')
        response3 = client.get("/blob?blob_hash=hash1&blob_type=fruit&fruit_name=banana")
        response4 = client.get("/blob?blob_type=fruit&fruit_name=banana&fruit_color=red")
        # check resp is as expected
        assert json.loads(r_no_args.data.decode("ASCII"))["error_message"] == 'Must provide blob_type'
        assert json.loads(r_no_type.data.decode("ASCII"))["error_message"] == 'Must provide blob_type'
        assert json.loads(r_return_all_entries_of_blobtype.data.decode("ASCII"))['body'] == {'entry_id':[1,2,3,4], 'blob_type':['fruit','fruit','fruit','fruit'],'fruit_name':['banana','apple','strawberry','banana'], 'fruit_color':['yellow','red','red','green'], 'blob_hash':['hash1','hash2','hash3','hash1']}
        assert json.loads(response1.data.decode("ASCII"))['body'] == {'entry_id':[2], 'blob_type':['fruit'],'fruit_name':['apple'], 'fruit_color':['red'], 'blob_hash':['hash2']}
        assert  json.loads(response2.data.decode("ASCII"))['body'] == {'entry_id':[2,3], 'blob_type':['fruit','fruit'],'fruit_name':['apple','strawberry'], 'fruit_color':['red','red'], 'blob_hash':['hash2','hash3']}
        assert  json.loads(response3.data.decode("ASCII"))['body'] == {'entry_id':[1,4], 'blob_type':['fruit','fruit'],'fruit_name':['banana','banana'], 'fruit_color':['yellow','green'], 'blob_hash':['hash1','hash1']}
        assert  json.loads(response4.data.decode("ASCII"))['body'] == {}
            

    def test_blob_post(self, client):
        """
        GIVEN a flask app
        WHEN a POST request with entry metadata is submitted to /blob
        THEN assert metadata + store_locations are added to db 
        """
        clear_all_tables()
        #create blob_hash
        test_blob = 'a perfectly passionate poem about pineapples'
        b_hash = 'hash1_pineapple'
        # POST to API: /blob endpoint, capture response
        response=client.post("/blob",data=json.dumps({'metadata':{'blob_type':'fruit','fruit_name':'pineapple','fruit_color':'yellow', 'blob_hash':b_hash}, 'paths':['path/to/blob', 'gs://blob_path/hash'], 'new':NEW_BLOB}),content_type='application/json')
        # check API response is of expected type 
        assert type(json.loads(response.data.decode("ASCII"))['body']['entry_id']) == int
        #check entry is in db
        with Session() as session: 
            resp = session.query(FruitTable).filter_by(fruit_name='pineapple').all()
            e_id = resp[0].entry_id
            assert len(resp) == 1
            assert type(resp[0]) == FruitTable
            assert resp[0].fruit_name == 'pineapple'
            resp = session.query(BlobTable).filter_by(blob_hash=b_hash).all()
            assert len(resp) == 2 

            # adding new save location for existing blob 
            response=client.post("/blob",data=json.dumps({'metadata':{'blob_type':'fruit','fruit_name':'pineapple','fruit_color':'yellow', 'blob_hash':b_hash}, 'paths':['new/path/to/blob'], 'new':NEW_LOCATION}),content_type='application/json')
            assert json.loads(response.data.decode("ASCII"))['body']['entry_id'] == e_id 
            path_count = session.query(BlobTable).filter_by(blob_hash=b_hash).count()
            metadata_count = session.query(FruitTable).filter_by(blob_hash=b_hash).count()
            assert path_count == 3
            assert metadata_count == 1


class TestUpdate:

    def test_update(self, client):
        clear_all_tables()
        with Session() as session: 
            # populate db
            b1 = BlobTable(blob_hash = 'hash1', blob_path='f/h1')
            b2 = BlobTable(blob_hash = 'hash2', blob_path='f/h2')
            b3 = BlobTable(blob_hash = 'hash3', blob_path='f/h3')
            banana = FruitTable(entry_id = 1,blob_type = 'fruit', fruit_name = 'banana', fruit_color= 'yellow',blob_hash= 'hash1')
            apple = FruitTable(entry_id = 2,blob_type = 'fruit', fruit_name = 'apple', fruit_color='red',blob_hash='hash2')
            strawberry = FruitTable(entry_id = 3,blob_type = 'fruit', fruit_name = 'strawberry', fruit_color='red',blob_hash='hash3')
            banana2 = FruitTable(entry_id = 4,blob_type = 'fruit', fruit_name = 'banana', fruit_color= 'green',blob_hash= 'hash1')
            session.add_all([b1,b2,b3, banana, apple, strawberry, banana2])
            session.commit() 
            e_id = session.query(FruitTable.entry_id).filter_by(fruit_name = 'apple').all()[-1][-1]
            # submit POST request with update
            r_valid1 = client.post('/blob/update', data=json.dumps({'blob_type':'fruit','current_entry_id':e_id,'update_data':{'fruit_color':'silver'}}),content_type='application/json') 
            # invalid or abscent:  blob_type,  entry_id, feild_change for given blob_type
            # assert returns expected 
            decode_valid1 = json.loads(r_valid1.data.decode("ASCII"))
            assert type(decode_valid1['body']['entry_id']) == int
            # TODO: use select to make sure new entry is there  
        

def test_get_fields(client):
    valid_resp = client.get('/fields?blob_type=fruit')
    invalid1_resp = client.get('/fields?invalidarg=fruit')
    invalid2_resp = client.get('/fields?blob_type=faketype')
    valid2_resp = client.get('/fields?blob_type=return_all_blob_types')
    assert json.loads(valid_resp.data.decode("ascii"))['status_code'] == 200
    assert json.loads(invalid1_resp.data.decode("ASCII"))["status_code"] == 400
    assert json.loads(invalid2_resp.data.decode("ASCII"))['status_code'] == 400
    assert json.loads(valid2_resp.data.decode('ascii'))['status_code'] == 200


def test_retrieve(client): #TODO Need to write supporting end point and library method
    clear_all_tables()
    with Session() as session, session.begin():
        pblob = BlobTable(blob_hash='hash1', blob_path='f/hash1')
        pineapple = FruitTable(entry_id = 101, blob_type='fruit',fruit_name='pineapple',fruit_color='yellow',blob_hash='hash1')
        session.add_all([pblob, pineapple])
    # send request, capture resp
    valid_resp = client.get('/blob/fruit/101')
    assert json.loads(valid_resp.data.decode("ascii"))['status_code'] == 200
    assert json.loads(valid_resp.data.decode("ascii"))['body'] == {'paths':['f/hash1']}
    


def test_clean():
    clear_all_tables()
    return True 