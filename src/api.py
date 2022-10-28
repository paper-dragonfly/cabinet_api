from flask import Flask, request
from pydantic import ValidationError
import api_fns as f
import json 

## NEXT STEP: create end points and supporting fns for library upload fn

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    return 'WELCOME TO CABINET'

@app.route('/blob', methods=['GET', 'POST'])
def blob_store():
    if request.method == 'GET':
        pass
    if request.method == 'POST':
        blob_bytes = request.get_json()['blob_bytes']
        blob_id = f.post_blob(blob_bytes) 
        return json.dumps({'blob_id':blob_id})

@app.route('/drawers', methods=['GET','POST'])
def drawers():
    pass 