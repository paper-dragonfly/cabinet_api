import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobPostData, UpdatePostData, blob_types, Response, RetrieveBlob

ENV = os.getenv('ENV')


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
                    api_resp = Response(status_code=400,error_message='Must provide blob_type',body=None)
                    return json.dumps(api_resp.dict())
                blob_type = user_search['blob_type']
                if not f.validate_search_fields(user_search):
                    api_resp= Response(status_code= 400,error_message= 'KeyError: invalid blob_type or search field', body=None)
                    return json.dumps(api_resp.dict())
                # blob_type only - return all entries for blob_type
                elif len(user_search) == 1:
                    matches = f.all_entries(blob_type, cur)
                    api_resp = Response(status_code= 200,error_message= None,body= matches)
                    return json.dumps(api_resp.dict())
                else:
                    matches:dict = f.search_metadata(blob_type,user_search,cur)
                    api_resp = Response(status_code= 200,error_message= None,body= matches)
                    return json.dumps(api_resp.dict())

            elif request.method == 'POST':
                try:
                    #extract info from POST
                    new_blob_info = BlobPostData.parse_obj(request.get_json()) 
                    blob_type = new_blob_info.metadata['blob_type']
                    if not blob_type in blob_types.keys():
                        api_resp = Response(status_code=400,error_message= f'{blob_type} blob_type does not exist', body=None)
                        return json.dumps(api_resp.dict())
                    metadata_inst = blob_types[blob_type].parse_obj(new_blob_info.metadata)
                    #add blob to db
                    blob_id = f.add_blob(new_blob_info.blob_b64s, cur)
                    # create dict with new_entry metadata including blob_id
                    metadata_inst.blob_id = blob_id 
                    metadata_dict = metadata_inst.dict()
                    # add metadata entry to db
                    entry_id = f.add_entry(blob_type, metadata_dict, cur)
                    api_resp = Response(status_code=200,error_message=None,body={'entry_id':entry_id})
                    return json.dumps(api_resp.dict())
                except (TypeError, ValueError) as e:
                    api_resp = Response(status_code=400, error_message= e, body=None)
                    return json.dumps(api_resp.dict())
        except:
            api_resp = Response(status_code=500, error_message='UnexpectedError', body=None)
            return json.dumps(api_resp.dict())
        finally:
            cur.close()
            conn.close()

    
    #TODO catch errors at /update how?
    @app.route('/update', methods=['POST'])
    def update():
        try:
            conn, cur = db_connect(env)
            try: 
                post_data = UpdatePostData.parse_obj(request.get_json())
                if not f.validate_update_fields(post_data.blob_type, post_data.update_data):
                    api_resp = Response(status_code= 400,error_message= 'KeyError: invalid blob_type or update fields', body=None)
                    return json.dumps(api_resp.dict())
                current_metadata = f.get_current_metadata(post_data.blob_type,post_data.current_entry_id,cur)
                full_update_dict = f.make_full_update_dict(post_data.update_data, current_metadata)
                updated_entry_id = f.add_entry(post_data.blob_type, full_update_dict, cur)
                api_resp = Response(status_code=200, error_message=None, body={'entry_id': updated_entry_id})
                return json.dumps(api_resp.dict())
            except (TypeError, ValueError) as e: 
                    api_resp = Response(status_code=400, error_message= e, body=None)
                    return json.dumps(api_resp.dict())
        except:
            api_resp = Response(status_code=500, error_message='UnexpectedError', body=None)
            return json.dumps(api_resp.dict())
        finally:
            cur.close()
            conn.close()
            return json.dumps(api_resp.dict())

    @app.route('/fields',methods=['GET'])
    def get_fields():
        """
        Return list of fields for specified blob_type
        """
        try:
            blob_type = request.args.to_dict()['blob_type']
        except:
            api_resp = Response(status_code = 400, error_message = 'Blob_TypeError: no blob_type given', body = None)
            return json.dumps(api_resp.dict())
        if blob_type not in blob_types.keys():
            api_resp = Response(status_code = 400, error_message = 'Blob_TypeError: invalid blob_type', body = None)
        else:
            bkeys = blob_types[blob_type].__fields__.keys() 
            api_resp = Response(status_code=200, error_message=None, body= {'fields':list(bkeys)})
        return json.dumps(api_resp.dict())

    
    @app.route('/blob/retrieve', methods=['GET'])
    def retrieve():
        """
        Retrun blob associate with submitted entry_id 
        """
        # confirm submitted args are valid
        pdb.set_trace()
        try:
            search_args =  RetrieveBlob.parse_obj(request.args.to_dict())
            search_dict = search_args.dict()
        except (TypeError, ValueError) as e: 
            api_resp = Response(status_code=400, error_message= e, body=None)
            return json.dumps(api_resp.dict())
        if search_args.blob_type not in blob_types.keys():
            api_resp = Response(status_code=400, error_message= "BlobTypeError: invalid blob_tpype", body=None)
            return json.dumps(api_resp.dict())
        # get blob
        try:
            conn, cur = db_connect(env=env)
            blob = f.retrieve_blob(search_dict,cur)
            api_resp = Response(status_code=200, error_message=None, body={'blob':blob})
            return json.dumps(api_resp.dict())
        except: 
            api_resp = Response(status_code=500, error_message='ConnectionError', body=None)
            return json.dumps(api_resp.dict())
        finally:
            cur.close()
            conn.close()
            
    return app


# if ENV != 'testing':
app = create_app(ENV)

if __name__ == '__main__':
    host = f.get_env_host(ENV)
    app.run('localhost', 5050, debug=True)