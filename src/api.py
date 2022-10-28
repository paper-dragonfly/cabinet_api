from flask import Flask, request
from pydantic import ValidationError
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
    def store_blob():
        if request.method == 'GET':
            pass
        if request.method == 'POST':
            blob_bytes = request.get_json()['blob_bytes']
            blob_id = f.add_blob(blob_bytes) 
            return json.dumps({'blob_id':blob_id})


    @app.route('/drawer', methods=['GET','POST'])
    def drawer():
        table_name = 'drawer'
        feilds = ['entry_id','photo_id','channel','title', 'blob_id']
        if request.method == 'GET':
            pass
        if request.method == 'POST': 
            post_dict = request.get_json()
            new = post_dict['new_blob']
            if new:
                entry_id = f.add_entry(table_name, post_dict['metadata'])
                return json.dumps({'status_code':200, 'entry_id':entry_id})
            else:
                pass
            
    return app 


if ENV != 'testing':
    app = create_app(ENV)

if __name__ == '__main__':
    host = f.get_env_host(ENV)
    app.run('localhost', 5050, debug=True)