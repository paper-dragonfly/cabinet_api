import pdb 
from typing import List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.api import create_app
from src.api_fns import db_connect
from src.database import TABLE_BLOB_TYPE_MATCHING

app = create_app('testing')
engine = create_engine('postgresql://katcha@localhost:5432/cabinet_test', echo=True)
Session = sessionmaker(bind=engine) 

@pytest.fixture
def client(): #sends the HTTP requests
    return app.test_client() 
    

def clear_all_tables():
    with Session() as session:
        for table in TABLE_BLOB_TYPE_MATCHING.values():
            session.query(table).delete() 
        session.commit() 
        
    
        

