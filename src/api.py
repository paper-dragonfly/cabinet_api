from crypt import methods
import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobInfo, blob_types

ENV = os.getenv('ENV')

## NEXT STEP: create end points and supporting fns for library upload fn

def create_app(env):
    app = Flask(__name__)


    @app.route('/home', methods=['GET'])
    def home():
        return 'WELCOME TO CABINET'


    # @app.route('/blob', methods=['GET', 'POST'])
    # #TODO: learn about bytea postgres type. 
    # def store_blob():
    #     if request.method == 'GET':
    #         raise NotImplementedError
    #     if request.method == 'POST':
    #         blob_bytes = request.get_json()['blob_bytes']
    #         blob_id = f.add_blob(blob_bytes) 
    #         return json.dumps({'blob_id':blob_id})

    # NOTE: each data type has its own end point with predefined columns
    # any changes to the table must be done in source code, not by user

    @app.route('/blob', methods=['GET', 'POST'])
    def blob(): 
        try:
            conn, cur = db_connect(env=env)
            if request.method == 'GET':
                user_search = request.args.to_dict()
                if not 'blob_type' in user_search.keys():
                    status_code = 400
                    payload = {'error_message':'must provide blob_type'}
                blob_type = user_search.pop('blob_type') 
                valid_fields = f.validate_search_fields(blob_type,user_search)
                if not valid_fields:
                    status_code = 400
                    payload = {'error_message':'Invalid blob_type or search field'}
                # no search args beyond blob_type - return all entries for blob_type
                elif not user_search:
                    matches = f.all_entries(blob_type, cur)
                    status_code = 200
                    payload = matches 
                else:
                    matches:dict = f.search_metadata(blob_type,user_search,cur)
                    status_code = 200
                    payload = matches
            elif request.method == 'POST':
                #extract info from POST
                new_blob_info:BlobInfo = BlobInfo.parse_obj(request.get_json()) 
                blob_type = new_blob_info.blob_type
                if not blob_type in blob_types.keys():
                    status_code = 400 
                    payload = f'{blob_type} table does not exist'
                blob_b64s = new_blob_info.blob_b64s
                # turn metadata into instance of specified metadata_class to enforce type hints
                metadata_inst = blob_types[blob_type].parse_obj(new_blob_info.metadata)
                # add blob to db 
                blob_id = f.add_blob(blob_b64s, cur)
                # create dict with new_entry metadata including blob_id
                metadata_inst.blob_id = blob_id 
                metadata_dict = metadata_inst.dict()
                # add metadata entry to db
                entry_id = f.add_entry(blob_type, metadata_dict, cur)
                status_code = 200
                payload = {'entry_id':entry_id}
        finally:
            cur.close()
            conn.close()
            return json.dumps({'status_code':status_code, 'body':payload})

    @app.route('/testtable', methods=['GET','POST'])
    def test_table():
        try:
            conn, cur = db_connect(env=env)
            blob_type = 'fruit'
            # feilds = ['entry_id','photo_id','channel','title', 'blob_id']
            feilds = ['entry_id','fruit_name','color','blob_id']
            if request.method == 'GET':
                raise NotImplementedError
            if request.method == 'POST': 
                post_data:AllTables = AllTables.parse_obj(request.get_json()) 
                new:bool = post_data.new_blob
                metadata:dict = post_data.metadata
                if new:
                    entry_id = f.add_entry(blob_type, metadata, cur, env)
                    return json.dumps({'status_code':200, 'entry_id':entry_id})
                else: #update
                    old_entry_id = post_data.old_entry_id
                    current_metadata:dict = f.get_current_metadata(blob_type, old_entry_id)
                    updated_metadata: dict = f.make_full_update_dict(metadata, current_metadata)
                    entry_id = f.add_entry(blob_type, updated_metadata, cur, env)
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