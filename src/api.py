import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobPostData, UpdatePostData, blob_types, Response, RetrieveBlob, blob_classes, Fields

ENV = os.getenv('ENV')


def create_app(env):
    app = Flask(__name__)


    @app.route('/home', methods=['GET'])
    def home():
        return 'WELCOME TO CABINET'


    @app.route('/blob/types', methods=['GET'])
    def list_blob_types():
        return dict(keys=blob_types, value=list[fields])

    @app.route('/blob', methods=['GET', 'POST'])
    def blob(): 
        try:
            conn, cur = db_connect(env=env)
            if request.method == 'GET':
                user_search = request.args.to_dict()
                if not 'blob_type' in user_search.keys():
                    api_resp = Response(status_code=400,error_message='Must provide blob_type')
                    return api_resp.json()
                blob_type = user_search['blob_type']
                if not f.validate_search_fields(user_search):
                    api_resp= Response(status_code= 400,error_message= 'KeyError: invalid blob_type or search field')
                    return api_resp.json()
                # blob_type only - return all entries for blob_type
                elif len(user_search) == 1:
                    matches = f.all_entries(blob_type, cur)
                    api_resp = Response(body= matches)
                    return api_resp.json()
                else:
                    matches:dict = f.search_metadata(blob_type,user_search,cur)
                    api_resp = Response(body= matches)
                    return api_resp.json()

            elif request.method == 'POST':
                try:
                    #extract info from POST
                    new_blob_info = BlobPostData.parse_obj(request.get_json()) 
                    blob_type = new_blob_info.metadata['blob_type']
                    if not blob_type in blob_types.keys():
                        api_resp = Response(status_code=400,error_message= f'{blob_type} blob_type does not exist', body=None)
                        return api_resp.json()
                    metadata_inst = blob_types[blob_type].parse_obj(new_blob_info.metadata)
                    #add blob to db
                    blob_id = f.add_blob(new_blob_info.blob_b64s, cur)
                    # create dict with new_entry metadata including blob_id
                    # TODO: rewrite add_entry to accept metadata_inst not dict
                    metadata_inst.blob_id = blob_id 
                    # add metadata entry to db
                    entry_id = f.add_entry(metadata_inst, cur)
                    api_resp = Response(body={'entry_id':entry_id})
                    return api_resp.json()
                except (TypeError, ValueError) as e:
                    api_resp = Response(status_code=400, error_message= e)
                    return api_resp.json()
        except:
            api_resp = Response(status_code=500, error_message='UnexpectedError')
            return api_resp.json()
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
                    api_resp = Response(status_code= 400,error_message= 'KeyError: invalid blob_type or update fields')
                    return api_resp.json()
                current_metadata = f.get_current_metadata(post_data.blob_type,post_data.current_entry_id,cur)
                full_update_inst = blob_types[post_data.blob_type].parse_obj(f.make_full_update_dict(post_data.update_data, current_metadata))
                updated_entry_id = f.add_entry(full_update_inst, cur)
                api_resp = Response(body={'entry_id': updated_entry_id})
                return api_resp.json()
            except (TypeError, ValueError) as e: 
                    api_resp = Response(status_code=400, error_message= e)
                    return api_resp.json()
        except:
            api_resp = Response(status_code=500, error_message='UnexpectedError')
            return api_resp.json()
        finally:
            cur.close()
            conn.close()
            return api_resp.json()

    @app.route('/fields',methods=['GET'])
    def get_fields():
        """
        Return list of fields for specified blob_type
        """
        try:
            blob_type = Fields.parse_obj(request.args.to_dict()).blob_type
        except Exception as e: 
            api_resp = Response(status_code=400, error_message= e.json())
            return api_resp.json()            
        if blob_type not in blob_types.keys() and blob_type != 'return_all_blob_types':
            api_resp = Response(status_code = 400, error_message = 'Blob_TypeError: invalid blob_type', body = request.args.to_dict())
        else:
            blob_types_list = [blob_type]
            if blob_type == 'return_all_blob_types':
                blob_types_list = blob_types.keys()
            types_and_fields = {}
            for t in blob_types_list:
                bkeys = list(blob_types[t].__fields__.keys())
                types_and_fields[t] = bkeys
            api_resp = Response(body= types_and_fields)
        return api_resp.json()

    
    @app.route('/blob/retrieve', methods=['GET'])
    def retrieve():
        """
        Retrun blob associate with submitted entry_id 
        """
        # confirm submitted args are valid
        try:
            search_args =  RetrieveBlob.parse_obj(request.args.to_dict())
            search_dict = search_args.dict()
        except (TypeError, ValueError) as e: 
            api_resp = Response(status_code=400, error_message= e)
            return api_resp.json()
        if search_args.blob_type not in blob_types.keys():
            api_resp = Response(status_code=400, error_message= "BlobTypeError: invalid blob_tpype", body=None)
            return api_resp.json()
        # get blob
        try:
            conn, cur = db_connect(env=env)
            blob = f.retrieve_blob(search_dict,cur)
            api_resp = Response(status_code=200, error_message=None, body={'blob':blob})
            return api_resp.json()
        except: 
            api_resp = Response(status_code=500, error_message='ConnectionError', body=None)
            return api_resp.json()
        finally:
            cur.close()
            conn.close()
            
    return app


# if ENV != 'testing':
app = create_app(ENV)

if __name__ == '__main__':
    host = f.get_env_host(ENV)
    app.run('localhost', 5050, debug=True)