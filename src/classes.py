from typing import Optional, Union

from pydantic import BaseModel

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

class PathsSchema(BaseModel):
    blob_type: str
    blob_hash: str 
    save_hosts: list 

class BlobPostSchema(BaseModel):
    metadata:dict
    paths: list
    
class BlobPutSchema(BaseModel):
    paths: list
    
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






