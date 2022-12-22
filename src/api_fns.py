from collections import UserString
import os
import pdb
from hashlib import sha256
from typing import List
from collections import defaultdict

import yaml

from src.constants import BLOB_TYPES, blob_classes, NEW_BLOB, NEW_LOCATION, DUPLICATE
from src.classes import StorageFnSchema
from src.database import BlobTable, TABLE_BLOB_TYPE_MATCHING


ENV = os.getenv("ENV")


def get_conn_str(env: str = ENV, config_file: str = "config/config.yaml") -> str:
    """
    Get string from config file to connect to postgreSQL database
    """
    with open(f"{config_file}", "r") as f:
        config_dict = yaml.safe_load(f)
    conn_str = config_dict[env]["conn_str"]
    return conn_str


def check_for_duplicate(storage_fn_inst: StorageFnSchema, session) -> str:
    """
    Checks if blob is already in Cabinet. If yes, is user trying to save to new location(s)?
    """
    blob_hash = storage_fn_inst.metadata["blob_hash"]
    matching_blobs = session.query(BlobTable).filter_by(blob_hash=blob_hash).all()
    # is blob already in cabinet?
    if not matching_blobs:
        return NEW_BLOB
    # is user requesting to save in a location where blob is already saved (i.e. duplicate)?
    saved_paths = [entry.blob_path for entry in matching_blobs]
    with open(f"config/config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)
    storage_options: dict = config_dict["storage_providers"][
        storage_fn_inst.metadata["blob_type"]
    ]
    for env in storage_fn_inst.storage_envs:
        for path in storage_options[env]:
            if path + "/" + blob_hash in saved_paths:
                return DUPLICATE
    # blob in cabinet but user requesting to save to new location
    return NEW_LOCATION


def generate_paths(new_blob_unsaved: StorageFnSchema) -> set:
    blob_type = new_blob_unsaved.metadata["blob_type"]
    blob_hash = new_blob_unsaved.metadata["blob_hash"]
    storage_envs: list = new_blob_unsaved.storage_envs
    with open(f"config/config.yaml", "r") as f:
        config_dict = yaml.safe_load(f)
    storage_options: dict = config_dict["storage_providers"][blob_type]
    paths = set()
    for env in storage_envs:
        for path in storage_options[env]:
            path = path + "/" + blob_hash
            paths.add(path)
    return paths


def add_blob_paths(blob_hash: str, paths: List[str], session) -> bool:
    """
    Stores sha256 hash for blobs alongside path to where blob is saved. The same blob may be saved in multiple places thus the same hash may appear in multiple entries pointing to different locations
    """
    try:
        for path in paths:
            blob_inst = BlobTable(blob_hash=blob_hash, blob_path=path)
            session.add(blob_inst)
        session.commit()
    except Exception:
        return False
    return True


def add_entry(parsed_metadata_inst: blob_classes, session) -> int:
    # take pydantic blob_type instance -> sqlalchemy BlobTable inst
    metadata_inst = TABLE_BLOB_TYPE_MATCHING[parsed_metadata_inst.blob_type](
        **parsed_metadata_inst.dict()
    )
    session.add(metadata_inst)
    session.commit()
    entry_id = metadata_inst.entry_id
    return entry_id


# __________________


def validate_search_fields(user_search: dict) -> bool:
    """
    Confirm blob_type is valid and that all search parameters are attributes of specified blob_type
    """
    # invalid blob_type?
    if not user_search["blob_type"] in BLOB_TYPES.keys():
        return False
    blob_type_fields = (
        BLOB_TYPES[user_search["blob_type"]].schema()["properties"].keys()
    )
    for key in user_search.keys():
        if not key in blob_type_fields:
            return False
    return True


def build_results_dict(blob_type: str, matches: List[blob_classes]) -> dict:
    """
    create dict with blob_type metadata column names as keys and list of values from matching entries as values
    """
    results = defaultdict(list)
    for match in matches:
        for key, val in match.__dict__.items():
            if key in BLOB_TYPES[blob_type].schema()["properties"].keys():
                results[key].append(val)
    return results


def all_entries(blob_type: str, session) -> dict:
    matches = [
        match for match in session.query(TABLE_BLOB_TYPE_MATCHING[blob_type]).all()
    ]
    return build_results_dict(blob_type, matches)


def search_metadata(blob_type: str, user_search: dict, session) -> dict:
    """
    Get all entries matching user's serach params
    """
    resp = session.query(TABLE_BLOB_TYPE_MATCHING[blob_type])
    for key, val in user_search.items():
        resp = resp.filter_by(**{key: val})
    results_dict = build_results_dict(blob_type, resp.all())
    return results_dict


# _______/update________


def validate_update_fields(blob_type: str, update_data: dict) -> dict:
    """
    Confirm blob_type is valid and that all update parameters are attributes of specified blob_type
    """
    # invalid blob_type?
    if not blob_type in BLOB_TYPES.keys():
        return {
            "valid": False,
            "error": f"BlobTypeError: {blob_type} is not a valid blob type",
        }
    blob_type_fields = BLOB_TYPES[blob_type].schema()["properties"].keys()
    invalid_fields = []
    for key in update_data.keys():
        if not key in blob_type_fields:
            invalid_fields.append(key)
    if invalid_fields:
        return {
            "valid": False,
            "error": f"FieldError: invalid fields for blob_type {blob_type} {invalid_fields}",
        }
    return {"valid": True}


def get_current_metadata(blob_type: str, entry_id: int, session) -> dict:
    current_metadata_raw = (
        session.query(TABLE_BLOB_TYPE_MATCHING[blob_type])
        .filter_by(entry_id=entry_id)
        .first()
        .__dict__
    )
    metadata = {}
    # remove extra '_sa_instance_state' key that's autogenerated
    for key in current_metadata_raw:
        if key in BLOB_TYPES[blob_type].schema()["properties"].keys():
            metadata[key] = current_metadata_raw[key]
    return metadata


def make_full_update_dict(updates: dict, current_metadata: dict):
    """
    Overwrite current_metadata with updates, remove entry_id key and return in new dict
    """
    full_update = {**current_metadata, **updates}
    del full_update["entry_id"]
    return full_update


# ________________


def retrieve_paths(search_args: dict, session) -> str:
    table = TABLE_BLOB_TYPE_MATCHING[search_args["blob_type"]]
    blob_hash = (
        session.query(table.blob_hash)
        .filter_by(entry_id=search_args["entry_id"])
        .first()[0]
    )
    matches = session.query(BlobTable.blob_path).filter_by(blob_hash=blob_hash)
    paths = []
    for match in matches:
        paths.append(match[0])
    return paths
