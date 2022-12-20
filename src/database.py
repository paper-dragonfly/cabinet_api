from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker


Base = declarative_base() 

class BlobTable(Base):
    __tablename__ = 'blob'

    blob_hash = Column(String)
    blob_path = Column(String, primary_key=True)
    
    def _repr_(self): #returns info in nice formatting
        return "<Blob(blob_hash = '%s', blob_path='%s')>" % (
            self.blob_hash,
            self.blob_path,
            )

class FruitTable(Base):
    __tablename__ = 'fruit'

    entry_id = Column(Integer, Sequence("fruit_entry_id_seq"), primary_key = True)
    blob_type = Column(String)
    fruit_name = Column(String)
    fruit_color = Column(String)
    blob_hash = Column(String)


    def _repr_(self): #returns info in nice formatting
        return "<Fruit(entry_id = '%s', blob_type='%s', fruit_name='%s', fruit_color='%s', blob_hash='%s')>" % (
            self.entry_id,
            self.blob_type,
            self.fruit_name,
            self.fruit_color,
            self.blob_hash
            )


class YoutubeTable(Base):
    __tablename__ = 'youtube'

    entry_id = Column(Integer, Sequence("youtube_entry_id_seq"), primary_key = True)
    blob_type = Column(String)
    blob_hash = Column(String) 
    photo_id = Column(String) 
    channel = Column(String)
    category = Column(String)
    title = Column(String)


TABLE_BLOB_TYPE_MATCHING = {'blob': BlobTable, 'fruit':FruitTable, 'youtube': YoutubeTable}