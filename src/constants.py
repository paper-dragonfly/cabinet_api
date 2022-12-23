from typing import Union
from src.classes import Fruit, Chess, Youtube

BLOB_TYPES = {"fruit": Fruit, "chess": Chess, "youtube": Youtube}
blob_classes = Union[Fruit, Chess, Youtube]

NEW_BLOB = "NEW_BLOB"
NEW_LOCATION = "NEW_LOCATION"
DUPLICATE = "DUPLICATE"
