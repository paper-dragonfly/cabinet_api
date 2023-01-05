import pdb
from typing import List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from src.api import create_app
from src.database import TABLE_BLOB_TYPE_MATCHING

app = create_app("testing")
engine = create_engine("postgresql://katcha@localhost:5432/cabinet_test", echo=True)
Session = sessionmaker(bind=engine)

client = TestClient(app)


def clear_all_tables():
    with Session() as session:
        for table in TABLE_BLOB_TYPE_MATCHING.values():
            session.query(table).delete()
        session.commit()
