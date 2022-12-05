import json 
import os
import pdb 

from flask import Flask, request
from pydantic import ValidationError

from src.api_fns import db_connect
import src.api_fns as f
from src.classes import Fruit, BlobPostData, UpdatePostData, Blob_Type, Response, RetrieveBlob, blob_classes, Fields

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
                    return Response(status_code=400,error_message='Must provide blob_type').json()
                blob_type = user_search['blob_type']
                if not f.validate_search_fields(user_search):
                    return Response(status_code= 400,error_message= 'KeyError: invalid blob_type or search field').json()
                # blob_type only - return all entries for blob_type
                elif len(user_search) == 1:
                    matches = f.all_entries(blob_type, cur)
                    return Response(body= matches).json()
                else:
                    matches:dict = f.search_metadata(blob_type,user_search,cur)
                    return Response(body= matches).json()

            elif request.method == 'POST':
                try:
                    new_blob_info = BlobPostData.parse_obj(request.get_json()) 
                    blob_type = new_blob_info.metadata['blob_type']
                    if not blob_type in Blob_Type.keys():
                        return Response(status_code=400,error_message= f'{blob_type} blob_type does not exist').json()
                    parsed_metadata = Blob_Type[blob_type].parse_obj(new_blob_info.metadata)
                    save_paths = f.generate_paths(parsed_metadata.blob_hash) 
                    # add paths to blob table (id = hash, path = cabinet_api/blobs/hash)
                    paths_added = f.add_blob_paths(parsed_metadata.blob_hash, save_paths,cur)
                    if not paths_added:
                        return Response(status_code=400, error_message='BlobDuplication: blob already in cabinet').json()
                    # add metadata entry to db
                    entry_id = f.add_entry(parsed_metadata, cur)
                    # send file_path(s) to SDK 
                    return Response(body={'paths':save_paths}).json()
                except (TypeError, ValueError) as e:
                    return Response(status_code=400, error_message= e).json()
        except:
            return Response(status_code=500, error_message='UnexpectedError').json
        finally:
            cur.close()
            conn.close()

    
    #TODO catch errors at /update how?
    @app.route('/blob/update', methods=['POST'])
    def update():
        try:
            conn, cur = db_connect(env)
            try:
                post_data = UpdatePostData.parse_obj(request.get_json())
                validation = f.validate_update_fields(post_data.blob_type, post_data.update_data)
                if not validation['valid']:
                    return Response(status_code= 400,error_message= validation['error']).json()
                current_metadata = f.get_current_metadata(post_data.blob_type,post_data.current_entry_id,cur)
                full_update_inst = Blob_Type[post_data.blob_type].parse_obj(f.make_full_update_dict(post_data.update_data, current_metadata))
                updated_entry_id = f.add_entry(full_update_inst, cur)
                return Response(body={'entry_id': updated_entry_id}).json()
            except (TypeError, ValueError) as e: 
                    return Response(status_code=400, error_message= e).json()
        except Exception as e:
            return Response(status_code=500, error_message= e.json()).json()
        finally:
            cur.close()
            conn.close()


    @app.route('/fields',methods=['GET'])
    def get_fields():
        """
        Return list of fields for specified blob_type
        """
        try:
            blob_type = Fields.parse_obj(request.args.to_dict()).blob_type
        except Exception as e: 
            return Response(status_code=400, error_message= e.json()).json()
        if blob_type not in Blob_Type.keys() and blob_type != 'return_all_blob_types':
            api_resp = Response(status_code = 400, error_message = 'Blob_TypeError: invalid blob_type', body = request.args.to_dict())
        else:
            blob_types_list = [blob_type]
            if blob_type == 'return_all_blob_types':
                blob_types_list = Blob_Type.keys()
            types_and_fields = {}
            for t in blob_types_list:
                bkeys = list(Blob_Type[t].__fields__.keys())
                types_and_fields[t] = bkeys
            api_resp = Response(body= types_and_fields)
        return api_resp.json()

    
    @app.route('/blob/<blob_type>/<id>', methods=['GET']) 
    def retrieve(blob_type=None, id=None):
        """
        Retrun list of locations where blob is saved
        """
        # confirm submitted args are valid
        try:
            search_args =  RetrieveBlob(blob_type=blob_type, entry_id=id)
            search_dict = search_args.dict()
        except (TypeError, ValueError) as e: 
            return Response(status_code=400, error_message= e).json()
        if search_args.blob_type not in Blob_Type.keys():
            return Response(status_code=400, error_message= "BlobTypeError: invalid blob_tpype").json()
        # get blob
        try:
            conn, cur = db_connect(env=env)
            paths = f.retrieve_paths(search_dict,cur)
            return Response(body={'paths':paths}).json()
        except: 
            return Response(status_code=500, error_message='ConnectionError').json()
        finally:
            cur.close()
            conn.close()
            
    return app


if __name__ == '__main__':
    if ENV:
        app = create_app(ENV)
        host, api_port = f.get_env_info(ENV)
    else: #defualt
        app = create_app('dev_local')
        host, api_port = f.get_env_info('dev_local')
    app.run(host, api_port, debug=True)