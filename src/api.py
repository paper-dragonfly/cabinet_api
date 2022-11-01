from crypt import methods
import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobInfo, metadata_classes

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
                raise NotImplementedError
            elif request.method == 'POST':
                #extract info from POST
                new_blob_info:BlobInfo = BlobInfo.parse_obj(request.get_json()) 
                table_name = new_blob_info.table_name
                if not table_name in metadata_classes.keys():
                    return json.dumps({'status_code':400, 'body':f'{table_name} table does not exist'})
                blob_bytes = new_blob_info.blob_bytes
                # turn metadata into instance of specified metadata_class to enforce type hints
                metadata_inst = metadata_classes[f'{table_name}'].parse_obj(new_blob_info.metadata)
                # add blob to db 
                blob_id = f.add_blob(blob_bytes, cur)
                # create dict with new_entry metadata including blob_id
                metadata_inst.blob_id = blob_id 
                metadata_dict = metadata_inst.dict()
                # add metadata entry to db
                entry_id = f.add_entry(table_name, metadata_dict, cur)
        finally:
            cur.close()
            conn.close()
            return json.dumps({'status_code':200, 'body':{'entry_id':entry_id}})

    @app.route('/testtable', methods=['GET','POST'])
    def test_table():
        try:
            conn, cur = db_connect(env=env)
            table_name = 'fruit'
            # feilds = ['entry_id','photo_id','channel','title', 'blob_id']
            feilds = ['entry_id','fruit_name','color','blob_id']
            if request.method == 'GET':
                raise NotImplementedError
            if request.method == 'POST': 
                post_data:AllTables = AllTables.parse_obj(request.get_json()) 
                new:bool = post_data.new_blob
                metadata:dict = post_data.metadata
                if new:
                    entry_id = f.add_entry(table_name, metadata, cur, env)
                    return json.dumps({'status_code':200, 'entry_id':entry_id})
                else: #update
                    old_entry_id = post_data.old_entry_id
                    current_metadata:dict = f.get_current_metadata(table_name, old_entry_id)
                    updated_metadata: dict = f.make_full_update_dict(metadata, current_metadata)
                    entry_id = f.add_entry(table_name, updated_metadata, cur, env)
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