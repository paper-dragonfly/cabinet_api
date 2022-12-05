import json
import pdb
import base64
from urllib import response
from hashlib import sha256

import pytest

from src.api import db_connect
import src.api as api
from tests.conftest import clear_all_tables

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
    response = client.get('/home') 
    assert response.data.decode("ASCII") == 'WELCOME TO CABINET'
    

def test_store_blob(client):
    """
    GIVEN a flask API 
    WHEN a dictionary containing a blob in bytes is POSTed to /blob
    THEN assert returns int (blob_hash)
    """
    pass 


class TestBlob:

    def test_blob_post(self, client):
        """
        GIVEN a flask app
        WHEN a POST request with entry metadata is submitted to /blob
        THEN assert metadata entry is added and API returns expected save_path
        """
        clear_all_tables()
        #create blob_b64s
        test_blob = 'a perfectly passionate poem about pineapples'
        blob_bytes = test_blob.encode('utf-8')
        b_hash = sha256(blob_bytes).hexdigest()
        # POST to API: /blob endpoint, capture response
        response=client.post("/blob",data=json.dumps({'metadata':{'blob_type':'fruit','fruit_name':'pineapple','fruit_color':'yellow', 'blob_hash':b_hash}}),content_type='application/json')
        # check API response is of expected type 
        resp = json.loads(response.data.decode("ASCII"))['body']
        expected_path = 'cabinet_api/blobs/'+b_hash
        assert resp['paths'] == [expected_path]
        #check entry is in db
        try: 
            conn, cur = db_connect('testing')
            cur.execute('SELECT * FROM fruit WHERE fruit_name=%s',('pineapple',))
            r = cur.fetchall()
            assert len(r) == 1
            assert len(r[0]) == 5
            assert 'pineapple' in r[0]
        finally:
            cur.close()
            conn.close() 

    def test_blob_get(self,client):
        """
        GIVEN a flask app 
        WHEN a GET request is sent to /blob containing search parameters as url query args
        THEN assert returns all entries matching search parameters 
        """
        clear_all_tables()
        try: 
            # populate_db 
            conn, cur = db_connect('testing')
            cur.execute("INSERT INTO blob(blob_hash, blob_path) VALUES('hash1','f/h1'),('hash2','f/h2'),('hash3','f/h3')")
            cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s)", ('1','fruit','banana','yellow','hash1','2','fruit','apple','red','hash2', '3','fruit','strawberry','red','hash3','4','fruit','banana','green','hash1'))
            # submit GET request
            # TODO 
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
            assert  json.loads(response4.data.decode("ASCII"))['body'] == None
        finally:
            cur.close()
            conn.close()

class TestUpdate:

    def test_update(self, client):
        clear_all_tables()
        try:
            # populate db with entries to update
            conn, cur = db_connect('testing')
            cur.execute("INSERT INTO blob(blob_hash, blob_path) VALUES('hash1','f/h1'),('hash2','f/h2'),('hash3','f/h3')")
            cur.execute("INSERT INTO fruit(blob_type,fruit_name, fruit_color, blob_hash) VALUES(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s)", ('fruit','banana','yellow','hash1','fruit','apple','red','hash2','fruit','strawberry','red','hash3','fruit','banana','green','hash1'))
            cur.execute("SELECT entry_id FROM fruit WHERE fruit_name = 'apple'")
            id = cur.fetchone()[0]
            # submit POST request with update
            r_valid1 = client.post('/blob/update', data=json.dumps({'blob_type':'fruit','current_entry_id':id,'update_data':{'fruit_color':'silver'}}),content_type='application/json') 
            # invalid or abscent:  blob_type,  entry_id, feild_change for given blob_type
            # assert returns expected 
            decode_valid1 = json.loads(r_valid1.data.decode("ASCII"))
            assert type(decode_valid1['body']['entry_id']) == int
            # TODO: use select to make sure new entry is there  
        finally:
            cur.close()
            conn.close()

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
    try:
        # populate db with blob and metadata
        conn, cur = db_connect('testing')
        cur.execute('INSERT INTO blob VALUES(%s, %s)',('hash1','blob_b64s pineapple'))
        cur.execute('INSERT INTO fruit(entry_id, fruit_name, blob_hash) VALUES(%s,%s,%s)',(101,'pineapple','hash1'))
        # send request, capture resp
        valid_resp = client.get('/blob/fruit/101')
        assert json.loads(valid_resp.data.decode("ascii"))['status_code'] == 200
        assert json.loads(valid_resp.data.decode("ascii"))['body'] == {'blob':'blob_b64s pineapple'}
    finally:
        cur.close()
        conn.close()


def test_clean():
    clear_all_tables()
    return True 