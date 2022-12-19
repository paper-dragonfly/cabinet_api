import pdb 
from typing import List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.api import create_app
from src.api_fns import db_connect

app = create_app('testing')
engine = create_engine('postgresql://katcha@localhost:5432/cabinet_test', echo=True)
Session = sessionmaker(bind=engine) 

@pytest.fixture
def client(): #sends the HTTP requests
    return app.test_client() 
    

def clear_tables(tables:list):
    try: 
        conn,cur = db_connect('testing')
        for table in tables:
            cur.execute(f'DELETE FROM {table}')
    finally:
        cur.close()
        conn.close()

def clear_all_tables():
    try:
        conn, cur= db_connect('testing')
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        table_names_raw:List[tuple] = cur.fetchall()
        table_names:list = [table_names_raw[i][0] for i in range(len(table_names_raw))]
        table_names.remove('blob')
        for t in table_names: 
            cur.execute(f"DELETE FROM {t}")
        cur.execute('DELETE FROM blob')
    finally:
        cur.close()
        conn.close()
        

