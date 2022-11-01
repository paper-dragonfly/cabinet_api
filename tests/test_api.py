import json
import pdb

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


def test_drawer_new(client):
    """
    GIVEN a flask app 
    WHEN a POST request with {'new':True} is submitted to /drawer
    THEN assert entry is added to db an int(entry_id) is returned 
    """
    #clear db 
    clear_all_tables()
    # submit POST request
    response=client.post("/testtable",data=json.dumps({'new_blob':True,'metadata':{'fruit_name':'pineapple','fruit_color':'yellow'}}),content_type='application/json')
    assert type(json.loads(response.data.decode("ASCII"))) == int 
    assert response.status_code == 200

def test_drawer_update(client):
    """
    GIVEN a flask app
    WHEN a POST request with {'new':False} is submitted to /drawer
    THEN assert entry is soft-updated in db and int(update entry_id) is returned
    """
    #clear db
    clear_all_tables()
    try:
        # populate with original entry
        conn, cur = db_connect('testing')
        cur.execute("INSERT INTO blob VALUES(%s)",(1,))
        cur.execute("INSERT INTO fruit VALUES(%s,%s,%s,%s)",(4,'plum','purple',1))
        # submit POST request
        response = client.post("/testtable", data=json.dumps({"new_blob":False,"metadata":{'fruit_color':'violet'}, 'old_entry_id':4}),content_type='application/json')
        assert response.status_code == 200 
        # assert 
    finally: 
        cur.close()
        conn.close() 

class TestBlob:
    clear_all_tables()

    def test_post_blob(client):
        """
        GIVEN a flask app
        WHEN a POST request with table, metadata and blob_bytes is submitted to /blob
        THEN assert returns int(entry_id)
        """
        test_blob = 'a perfectly passionate poem about pineapples'
        blob_bytes = test_blob.encode('utf-8') 
        response=client.post("/blob",data=json.dumps({'table':'fruit', 'metadata':{'fruit_name':'pineapple','fruit_color':'yellow'}, 'blob_bytes':blob_bytes}),content_type='application/json')
        # check API response is of expected type 
        assert type(json.loads(response.data.decode("ASCII"))) == int 
        #check entry is in db
        try: 
            conn, cur = db_connect('testing')
            cur.execute('SELECT * FROM fruit WHERE fruit_name=%s',('pineapple',))
            r = cur.fetchall()
            assert len(r) == 1
            assert 'pineapple' in r[0]
        finally:
            cur.close()
            conn.close() 


