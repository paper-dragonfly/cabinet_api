from typing import Optional
from pydantic import BaseModel

# NOTE
# 1. blob_id default to 0 to allow for initial type-hint enforcement before blob added to db


class BlobPostData(BaseModel):
    metadata:dict
    blob_b64s:str

class UpdatePostData(BaseModel):
    blob_type: str
    current_entry_id:int 
    update_data: dict 

class Response(BaseModel):
    status_code:int 
    error_message:str = None
    body:dict = None 

# Blob Types 

class Fruit(BaseModel):
    entry_id:Optional[int]
    blob_type:str 
    fruit_name:str 
    fruit_color: Optional[str]
    blob_id:str = None


blob_types = {'fruit':Fruit}