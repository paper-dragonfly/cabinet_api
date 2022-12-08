from typing import Union
from src.classes import Fruit, Chess, Youtube 

# Blob_type Record 
BLOB_TYPES = {'fruit':Fruit, 'chess':Chess, 'youtube':Youtube}
blob_classes = Union[Fruit, Chess, Youtube]