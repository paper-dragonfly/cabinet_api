import pdb
from http import HTTPStatus
from typing import Union

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, Request

import src.api_fns as f
from src.classes import (
    BlobPostSchema,
    UpdatePostSchema,
    Response,
    RetrieveBlob,
    StorageFnSchema,
)
from src.constants import BLOB_TYPES, NEW_BLOB, DUPLICATE


def create_app(env):
    # app = Flask(__name__)
    app = FastAPI()
    conn_str = f.get_conn_str(env)
    engine = create_engine(conn_str, echo=True)
    Session = sessionmaker(bind=engine)

    @app.get("/health")
    def read_health():
        return {"status": HTTPStatus.OK}

    @app.get("/")
    def read_home():
        return "WELCOME TO CABINET"

    @app.get("/store_envs")
    def read_store_envs(blob_type: str):
        with open("config/config.yaml", "r") as file:
            config_dict = yaml.safe_load(file)
        envs = list(config_dict["storage_providers"][blob_type].keys())
        return Response(body={"envs": envs})

    @app.post("/storage_urls")
    def create_storage_urls(new_blob_unsaved: StorageFnSchema):
        try:
            # confirm metadata matches blob_type schema - will throw error if fails
            blob_type = new_blob_unsaved.metadata["blob_type"]
            blob_metadata = BLOB_TYPES[blob_type].parse_obj(new_blob_unsaved.metadata)
            # confirm not duplicate
            with Session() as session:
                blob_cabinet_relationship = f.check_for_duplicate(
                    new_blob_unsaved, session
                )
            if blob_cabinet_relationship == DUPLICATE:
                return Response(
                    status_code=400,
                    error_message="BlobDuplication: blob already saved in requested location",
                )
            save_paths = f.generate_paths(new_blob_unsaved)
            return Response(
                body={"paths": save_paths, "new": blob_cabinet_relationship}
            )
        except Exception as e:
            return Response(status_code=400, error_message=e)

    @app.get("/blob")
    def read_blob(request: Request):
        try:
            user_search = request.query_params
            if not "blob_type" in user_search.keys():
                return Response(status_code=400, error_message="Must provide blob_type")
            if not f.validate_search_fields(user_search):
                return Response(
                    status_code=400,
                    error_message="KeyError: invalid blob_type or search field",
                )
            # blob_type only - return all entries for blob_type
            with Session() as session:
                if len(user_search) == 1:
                    matches = f.all_entries(user_search["blob_type"], session)
                    return Response(body=matches)
                else:
                    matches: dict = f.search_metadata(
                        user_search["blob_type"], user_search, session
                    )
                return Response(body=matches)
        except Exception as e:
            return Response(status_code=500, error_message=f"UnexpectedError: {e}").json

    @app.post("/blob")
    def create_blob(new_blob_info: BlobPostSchema):
        try:
            blob_type = new_blob_info.metadata["blob_type"]
            parsed_metadata = BLOB_TYPES[blob_type].parse_obj(new_blob_info.metadata)
            # add paths to blob table (id = hash, path = blobs/blob_type/hash)
            with Session() as session:
                paths_added = f.add_blob_paths(
                    parsed_metadata.blob_hash, new_blob_info.paths, session
                )
                if not paths_added:
                    return Response(status_code=500, error_message="Error adding paths")
                # add metadata entry to db
                if new_blob_info.new == NEW_BLOB:
                    entry_id = f.add_entry(parsed_metadata, session)
                else:  # id most up-to-date metadata for blob
                    entry_id = max(
                        f.search_metadata(
                            blob_type,
                            {"blob_hash": parsed_metadata.blob_hash},
                            session,
                        )["entry_id"]
                    )
                return Response(body={"entry_id": entry_id})
        except Exception as e:
            return Response(status_code=400, error_message=e)

    @app.post("/blob/update")
    def create_update(post_data: UpdatePostSchema):
        with Session() as session:
            try:
                validation = f.validate_update_fields(
                    post_data.blob_type, post_data.update_data
                )
                if not validation["valid"]:
                    return Response(status_code=400, error_message=validation["error"])
                current_metadata = f.get_current_metadata(
                    post_data.blob_type, post_data.current_entry_id, session
                )
                full_update_inst = BLOB_TYPES[post_data.blob_type].parse_obj(
                    f.make_full_update_dict(post_data.update_data, current_metadata)
                )
                updated_entry_id = f.add_entry(full_update_inst, session)
                return Response(body={"entry_id": updated_entry_id})
            except (TypeError, ValueError) as e:
                return Response(status_code=400, error_message=e)

    @app.get("/fields")
    def read_fields(blob_type: str):
        """
        Return list of fields for specified blob_type
        """
        if blob_type not in BLOB_TYPES.keys() and blob_type != "return_all_blob_types":
            api_resp = Response(
                status_code=400,
                error_message="BLOB_TYPESError: invalid blob_type",
                body={"blob_type": blob_type},
            )
        else:
            blob_types_list = [blob_type]
            if blob_type == "return_all_blob_types":
                blob_types_list = BLOB_TYPES.keys()
            types_and_fields = {}
            for t in blob_types_list:
                bkeys = list(BLOB_TYPES[t].__fields__.keys())
                types_and_fields[t] = bkeys
            api_resp = Response(body=types_and_fields)
        return api_resp

    @app.get("/blob/{blob_type}/{id}")
    def read_retrieve(blob_type: str, id: int):
        """
        Retrun list of locations where blob is saved
        """
        # confirm submitted args are valid
        try:
            search_args = RetrieveBlob(blob_type=blob_type, entry_id=id)
            search_dict = search_args.dict()
        except (TypeError, ValueError) as e:
            return Response(status_code=400, error_message=e)
        if search_args.blob_type not in BLOB_TYPES.keys():
            return Response(
                status_code=400, error_message="BlobTypeError: invalid blob_type"
            )
        # get blob paths
        with Session() as session:
            paths = f.retrieve_paths(search_dict, session)
            return Response(body={"paths": paths})

    return app
