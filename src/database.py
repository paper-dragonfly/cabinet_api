from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine(conn_str, echo=True)

Base = declarative_base() 

class Blob(Base):
    __tablename__ = 'blob'

    blob_hash = Column(String)
    blob_path = Column(String, primary_key=True)
    
    def _repr_(self):
        return "<Blob(blob_hash = '%s', blob_path='%s')>" % (
            self.blob_hash,
            self.blob_path,
            )


def create_tables(env):