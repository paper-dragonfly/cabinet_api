from pydantic import BaseModel

class AllTables(BaseModel):
    new_blob:bool 
    metadata:dict
    old_entry_id:int = None

class Fruit(BaseModel):
    fruit_name:str
    fruit_color: str
    blob_id: int