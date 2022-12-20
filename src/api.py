import json 
import os
import pdb 
from http import HTTPStatus

from flask import Flask, request
from pydantic import ValidationError
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker 


from src.api_fns import db_connect
import src.api_fns as f
from src.classes import BlobPostSchema, BlobPutSchema, UpdatePostSchema, Response, RetrieveBlob, Fields, StorageFnSchema
from src.constants import blob_classes, BLOB_TYPES, NEW_BLOB, NEW_LOCATION, DUPLICATE


def create_app(env):
    app = Flask(__name__)
    conn_str = f.get_conn_str(env)
    engine = create_engine(conn_str, echo=True)
    Session = sessionmaker(bind=engine)

    @app.route('/health', methods=['GET'])
    def health():
        return {"status": HTTPStatus.OK}


    @app.route('/', methods=['GET'])
    def home():
        return 'WELCOME TO CABINET'


    @app.route('/store_envs', methods=['GET'])
    def store_envs():
        blob_type = request.args.to_dict()['blob_type']
        with open('config/config.yaml','r') as file:
            config_dict = yaml.safe_load(file) 
        envs = list(config_dict['storage_providers'][blob_type].keys())
        return Response(body={'envs':envs}).json() 


    @app.route('/generate_storage_urls', methods=['POST'])
    def generate_storage_urls():
        with Session() as session:
            try: 
                new_blob_unsaved = StorageFnSchema.parse_obj(request.get_json())
                # confirm metadata matches blob_type schema
                blob_type = new_blob_unsaved.metadata['blob_type']
                blob_metadata = BLOB_TYPES[blob_type].parse_obj(new_blob_unsaved.metadata)
                # confirm not duplicate
                blob_cabinet_relationship = f.check_for_duplicate(new_blob_unsaved, session)
                if blob_cabinet_relationship == DUPLICATE:
                    return Response(status_code=400, error_message='BlobDuplication: blob already saved in requested location').json()
                save_paths = f.generate_paths(new_blob_unsaved)
                return Response(body={'paths':save_paths, 'new':blob_cabinet_relationship}).json()
            except Exception as e:
                return Response(status_code=400, error_message= e).json()
            

    @app.route('/blob', methods=['GET', 'POST'])
    def blob():     
        with Session() as session:    

            if request.method == 'GET':
                try: 
                    user_search = request.args.to_dict()
                    if not 'blob_type' in user_search.keys():
                        return Response(status_code=400,error_message='Must provide blob_type').json()
                    if not f.validate_search_fields(user_search):
                        return Response(status_code= 400,error_message= 'KeyError: invalid blob_type or search field').json()
                    # blob_type only - return all entries for blob_type
                    elif len(user_search) == 1:
                        matches = f.all_entries(user_search['blob_type'], session)
                        return Response(body= matches).json()
                    else:
                        matches:dict = f.search_metadata(user_search['blob_type'],user_search,session)
                    return Response(body= matches).json()
                except Exception as e:
                    return Response(status_code=500, error_message= f'UnexpectedError: {e}').json

            elif request.method == 'POST':
                try:
                    new_blob_info = BlobPostSchema.parse_obj(request.get_json()) 
                    blob_type = new_blob_info.metadata['blob_type']
                    parsed_metadata = BLOB_TYPES[blob_type].parse_obj(new_blob_info.metadata)
                    # add paths to blob table (id = hash, path = blobs/blob_type/hash)
                    paths_added = f.add_blob_paths(parsed_metadata.blob_hash, new_blob_info.paths ,cur)
                    if not paths_added:
                        return Response(status_code=500, error_message='Error adding paths').json()
                    # add metadata entry to db
                    if new_blob_info.new == NEW_BLOB:
                        entry_id = f.add_entry(parsed_metadata, cur)
                    else: #id most up-to-date metadata for blob
                        entry_id = max(f.search_metadata(blob_type, {'blob_hash':parsed_metadata.blob_hash},session)['entry_id'])
                    return Response(body={'entry_id':entry_id}).json()
                except Exception as e:
                    return Response(status_code=400, error_message= e).json()

    
    #TODO catch errors at /update how?
    @app.route('/blob/update', methods=['POST'])
    def update():
        try:
            conn, cur = db_connect(env)
            try:
                post_data = UpdatePostSchema.parse_obj(request.get_json())
                validation = f.validate_update_fields(post_data.blob_type, post_data.update_data)
                if not validation['valid']:
                    return Response(status_code= 400,error_message= validation['error']).json()
                current_metadata = f.get_current_metadata(post_data.blob_type,post_data.current_entry_id,cur)
                full_update_inst = BLOB_TYPES[post_data.blob_type].parse_obj(f.make_full_update_dict(post_data.update_data, current_metadata))
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
        if blob_type not in BLOB_TYPES.keys() and blob_type != 'return_all_blob_types':
            api_resp = Response(status_code = 400, error_message = 'BLOB_TYPESError: invalid blob_type', body = request.args.to_dict())
        else:
            blob_types_list = [blob_type]
            if blob_type == 'return_all_blob_types':
                blob_types_list = BLOB_TYPES.keys()
            types_and_fields = {}
            for t in blob_types_list:
                bkeys = list(BLOB_TYPES[t].__fields__.keys())
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
        if search_args.blob_type not in BLOB_TYPES.keys():
            return Response(status_code=400, error_message= "BlobTypeError: invalid blob_type").json()
        # get blob paths
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

