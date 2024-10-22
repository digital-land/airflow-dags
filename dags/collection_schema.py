from enum import Enum
from typing import List, Union, Dict, Optional

from pydantic import BaseModel, StrictStr


class CollectionSelection(str, Enum):
    none = "none"
    all = "all"
    explicit = "explicit"


class ScheduledCollectionConfig(BaseModel):
    selection: CollectionSelection
    collections: List[StrictStr] = []
    schedule: Optional[StrictStr] = None
    max_active_tasks: Optional[int] = 100


class Environments(BaseModel):
    development: ScheduledCollectionConfig
    staging: ScheduledCollectionConfig
    production: ScheduledCollectionConfig

    def for_env(self, env) -> ScheduledCollectionConfig:
        return self.__dict__[env]
