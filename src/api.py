from flask import Flask, request
from pydantic import ValidationError
from src.api_fns import db_connect
import src.api_fns as f
import json 
import os
import pdb 

ENV = os.getenv('ENV')

## NEXT STEP: create end points and supporting fns for library upload fn

def create_app(env):
    app = Flask(__name__)


    @app.route('/home', methods=['GET'])
    def home():
        return 'WELCOME TO CABINET'


    @app.route('/blob', methods=['GET', 'POST'])
    #TODO: learn about bytea postgres type. 
    def store_blob():
        if request.method == 'GET':
            pass
        if request.method == 'POST':
            blob_bytes = request.get_json()['blob_bytes']
            blob_id = f.add_blob(blob_bytes) 
            return json.dumps({'blob_id':blob_id})

    # NOTE: each data type has its own end point with predefined columns
    # any changes to the table must be done in source code, not by user

    @app.route('/drawer', methods=['GET','POST'])
    def drawer():
        try:
            conn, cur = db_connect()
            table_name = 'drawer'
            feilds = ['entry_id','photo_id','channel','title', 'blob_id']
            if request.method == 'GET':
                pass
            if request.method == 'POST': 
                post_data = request.get_json()
                new:bool = post_data['new_blob']
                if new:
                    entry_id = f.add_entry(table_name, post_data['metadata'])
                    return json.dumps({'status_code':200, 'entry_id':entry_id})
                else:
                    updates:dict = post_data['update_dict']
                    current_metadata:dict = f.get_current_metadata(table_name, entry_id)
                    updated_metadata: dict = f.make_full_update_dict(updates, current_metadata)
                    entry_id = f.add_entry(table_name, updated_metadata)
                    return json.dumps({'status_code':200, 'entry_id': entry_id})
        finally:
            cur.close()
            conn.close()
            
    return app 


if ENV != 'testing':
    app = create_app(ENV)

if __name__ == '__main__':
    host = f.get_env_host(ENV)
    app.run('localhost', 5050, debug=True)