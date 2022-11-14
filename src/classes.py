from typing import Optional, Union
from pydantic import BaseModel

# NOTE
# 1. blob_id default to 0 to allow for initial type-hint enforcement before blob added to db

# Blob Types 

class Fruit(BaseModel):
    entry_id:Optional[int]
    blob_type:str 
    fruit_name:str 
    fruit_color: Optional[str]
    blob_id:str = None

class Chess(BaseModel):
    entry_id: Optional[int]
    blob_type:str = 'chess'

blob_types = {'fruit':Fruit, 'chess':Chess}
blob_classes = Union[Fruit, Chess]

# endpoint inputs 

class BlobGetData(BaseModel):
    pass 

class BlobPostData(BaseModel):
    metadata:dict
    blob_b64s:str


class UpdatePostData(BaseModel):
    blob_type: str
    current_entry_id:int 
    update_data: dict 

class Fields(BaseModel):
    blob_type: str

class RetrieveBlob(BaseModel):
    blob_type: str
    entry_id: int 

# Response

class Response(BaseModel):
    status_code:int = 200
    error_message:str = None
    body:dict = None 






