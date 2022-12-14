from typing import Optional, Union

from pydantic import BaseModel, ValidationError, validator

# NOTE
# 1. blob_hash default to 0 to allow for initial type-hint enforcement before blob added to db

# Blob Types 

class Fruit(BaseModel):
    entry_id:Optional[int]
    blob_type:str 
    fruit_name:str 
    fruit_color: Optional[str]
    blob_hash:str 

class Chess(BaseModel):
    entry_id: Optional[int]
    blob_type:str = 'chess'

class Youtube(BaseModel):
    entry_id: Optional[int]
    blob_type:str = 'youtube'
    blob_hash:str  
    photo_id: str 
    channel: str
    category: str
    title:str 

# endpoint inputs 
blob_classes = [Fruit, Chess, Youtube]
BLOB_TYPES = {'fruit':Fruit, 'chess':Chess, 'youtube':Youtube}


class StorageFnSchema(BaseModel):
    metadata: dict
    storage_envs: list 

    @validator('metadata')
    def blobtype_hash_in_metadata(cls, v):
        if 'blob_type' not in v.keys():
            raise KeyError('metadata must include blob_type') 
        if v['blob_type'] not in BLOB_TYPES.keys():
            raise ValueError(f"InvalidBlobType: {v['blob_type']} blob_type does not exist")
        if 'blob_hash' not in v.keys():
            raise KeyError('metadata must include blob_hash')
        return v 

class BlobPostSchema(BaseModel):
    metadata:dict
    paths: list 
    new: str


    @validator('metadata')
    def valid_blobtype(cls, v):
        if 'blob_type' not in v.keys():
            raise KeyError('metadata must include blob type') 
        if v['blob_type'] not in BLOB_TYPES.keys():
            raise ValueError(f"InvalidBlobType: {v['blob_type']} blob_type does not exist")
        return v 

    
class UpdatePostSchema(BaseModel):
    blob_type: str
    current_entry_id:int 
    update_data: dict 

class Fields(BaseModel):
    blob_type:str

class RetrieveBlob(BaseModel):
    blob_type: str
    entry_id: int 

# Response
class Response(BaseModel):
    status_code:int = 200
    error_message:str = None
    body:dict = None 






