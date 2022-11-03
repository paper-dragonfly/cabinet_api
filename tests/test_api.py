import json
import pdb
import base64
from urllib import response

import pytest

from src.api import db_connect
import src.api as api
from conftest import clear_all_tables

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
    THEN assert returns int (blob_id)
    """
    pass 


# def test_drawer_update(client):
#     """
#     GIVEN a flask app
#     WHEN a POST request with {'new':False} is submitted to /drawer
#     THEN assert entry is soft-updated in db and int(update entry_id) is returned
#     """
#     #clear db
#     clear_all_tables()
#     try:
#         # populate with original entry
#         conn, cur = db_connect('testing')
#         cur.execute("INSERT INTO blob VALUES(%s)",(1,))
#         cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s)",(4,'plum','purple',1))
#         # submit POST request
#         response = client.post("/testtable", data=json.dumps({"new_blob":False,"metadata":{'fruit_color':'violet'}, 'old_entry_id':4}),content_type='application/json')
#         assert response.status_code == 200 
#         # assert 
#     finally: 
#         cur.close()
#         conn.close() 

class TestBlob:
    clear_all_tables()

    def test_post_blob(self, client):
        """
        GIVEN a flask app
        WHEN a POST request with table, metadata and blob_b64s (blob->str(base64)) is submitted to /blob
        THEN assert returns int(entry_id)
        """
        #create blob_b64s
        test_blob = 'a perfectly passionate poem about pineapples'
        blob_bytes = test_blob.encode('utf-8')
        blob_base64 = base64.b64encode(blob_bytes)
        blob_b64s = str(blob_base64)
        # POST to API: /blob endpoint, capture response
        response=client.post("/blob",data=json.dumps({'blob_type':'fruit', 'metadata':{'fruit_name':'pineapple','fruit_color':'yellow'}, 'blob_b64s':blob_b64s}),content_type='application/json')
        # check API response is of expected type 
        assert type(json.loads(response.data.decode("ASCII"))) == int 
        #check entry is in db
        try: 
            conn, cur = db_connect('testing')
            cur.execute('SELECT * FROM fruit WHERE fruit_name=%s',('pineapple',))
            r = cur.fetchall()
            assert len(r) == 1
            assert len(r[0]) == 4
            assert 'pineapple' in r[0]
        finally:
            cur.close()
            conn.close() 

    def test_get_blob(self,client):
        """
        GIVEN a flask app 
        WHEN a GET request is sent to /blob containing search parameters as url query args
        THEN assert returns all entries matching search parameters 
        """
        clear_all_tables()
        try: 
            # populate_db 
            conn, cur = db_connect('testing')
            cur.execute("INSERT INTO blob(blob_id) VALUES('hash1'),('hash2'),('hash3')")
            cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s),(%s,%s,%s,%s)", ('1','banana','yellow','hash1','2','apple','red','hash2', '3','strawberry','red','hash3','4','banana','green','hash1'))
            # submit GET request
            # TODO 
            r_no_args = client.get("/blob")
            r_no_type = client.get('/blob?fruit_name=banana')
            r_no_search = client.get("/blob?blob_type=fruit")
            response1 = client.get("/blob?blob_type=fruit&entry_id=2")
            response2 = client.get('/blob?blob_type=fruit&fruit_color=red')
            response3 = client.get("/blob?blob_type=fruit&fruit_name=banana&blob_id=hash1")
            response4 = client.get("/blob?blob_type=fruit&fruit_name=banana&fruit_color=red")

            # check resp is as expected
            assert json.loads(r_no_args.data.decode("ASCII"))["body"] == {'error_message':'must provide blob_type'}
            assert json.loads(r_no_type.data.decode("ASCII"))['body'] == {'error_message':'must provide blob_type'}
            assert json.loads(r_no_search.data.decode("ASCII"))['body'] == {'entry_id':[1,2,3,4], 'fruit_name':['banana','apple','strawberry','banana'], 'fruit_color':['yellow','red','red','green'], 'blob_id':['hash1','hash2','hash3','hash1']}
            assert json.loads(response1.data.decode("ASCII"))['body'] == {'entry_id':[2], 'fruit_name':['apple'], 'fruit_color':['red'], 'blob_id':['hash2']}
            assert  json.loads(response2.data.decode("ASCII"))['body'] == {'entry_id':[2,3], 'fruit_name':['apple','strawberry'], 'fruit_color':['red','red'], 'blob_id':['hash2','hash3']}
            assert  json.loads(response3.data.decode("ASCII"))['body'] == {'entry_id':[1,4], 'fruit_name':['banana','banana'], 'fruit_color':['yellow','green'], 'blob_id':['hash1','hash1']}
            assert  json.loads(response4.data.decode("ASCII"))['body'] == None
        finally:
            cur.close()
            conn.close()
