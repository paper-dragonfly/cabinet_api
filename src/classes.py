from typing import Optional
from pydantic import BaseModel

# NOTE
# 1. blob_id default to 0 to allow for initial type-hint enforcement before blob added to db


class BlobInfo(BaseModel):
    table_name:str
    metadata:dict
    blob_bytes:bytes

class Fruit(BaseModel):
    entry_id:Optional[int]
    fruit_name:str 
    fruit_color: Optional[str]
    blob_id:int = 0


metadata_classes = {'fruit':Fruit}