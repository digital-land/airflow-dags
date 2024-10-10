from enum import Enum
from typing import List, Union, Dict, Optional

from pydantic import BaseModel, StrictStr


class CollectionSelection(str, Enum):
    none = "none"
    all = "all"
    explicit = "explicit"


class CollectionConfig(BaseModel):
    selection: CollectionSelection
    collections: List[StrictStr] = []
    schedule: Optional[StrictStr] = None


class Environments(BaseModel):
    development: CollectionConfig
    staging: CollectionConfig
    production: CollectionConfig

    def for_env(self, env) -> CollectionConfig:
        return self.__dict__[env]
