from crypt import methods
import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobPostData, UpdatePostData, blob_types, Response

ENV = os.getenv('ENV')

## NEXT STEP: create end points and supporting fns for library upload fn

def create_app(env):
    app = Flask(__name__)


    @app.route('/home', methods=['GET'])
    def home():
        return 'WELCOME TO CABINET'


    @app.route('/blob', methods=['GET', 'POST'])
    def blob(): 
        try:
            conn, cur = db_connect(env=env)
            if request.method == 'GET':
                user_search = request.args.to_dict()
                if not 'blob_type' in user_search.keys():
                    api_resp = Response(status_code=200,error_message='Must provide blob_type',body=None)
                blob_type = user_search['blob_type']
                if not f.validate_search_fields(user_search):
                    api_resp = Response(status_code= 400,error_message= 'KeyError: invalid blob_type or search field', body=None)
                # blob_type only - return all entries for blob_type
                elif len(user_search) == 1:
                    matches = f.all_entries(blob_type, cur)
                    api_resp = Response(status_code= 200,error_message= None,body= matches)
                else:
                    matches:dict = f.search_metadata(blob_type,user_search,cur)
                    api_resp = Response(status_code= 200,error_message= None,body= matches)

            elif request.method == 'POST':
                #extract info from POST
                new_blob_info:BlobPostData = BlobPostData.parse_obj(request.get_json()) 
                blob_type = new_blob_info.metadata['blob_type']
                if not blob_type in blob_types.keys():
                    api_resp = Response(400,f'{blob_type} blob_type does not exist',None)
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
                api_resp = Response(status_code=200,error_message=None,body={'entry_id':entry_id})
        finally:
            cur.close()
            conn.close()
            return json.dumps(api_resp.dict())

    
    @app.route('/update', methods=['POST'])
    def update():
        try:
            conn, cur = db_connect(env)
            post_data = UpdatePostData.parse_obj(request.get_json())
            current_metadata = f.get_current_metadata(post_data.blob_type,post_data.current_entry_id,cur)
            full_update_dict = f.make_full_update_dict(post_data.update_data, current_metadata)
            updated_entry_id = f.add_entry(post_data.blob_type, full_update_dict, cur)
            status_code = 200
            payload = {'entry_id': updated_entry_id} 
        finally:
            cur.close()
            conn.close()
            return json.dumps({'status_code':status_code, 'body':payload})
            
    return app 


if ENV != 'testing':
    app = create_app(ENV)

if __name__ == '__main__':
    host = f.get_env_host(ENV)
    app.run('localhost', 5050, debug=True)