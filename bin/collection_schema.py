from enum import Enum
from typing import List, Union, Dict

from pydantic import BaseModel, StrictStr


class CollectionSelection(str, Enum):
    none = "none"
    all = "all"


class CollectionConfig(BaseModel):
    development: Union[CollectionSelection, List[StrictStr]]
    staging: Union[CollectionSelection, List[StrictStr]]
    production: Union[CollectionSelection, List[StrictStr]]

    def for_env(self, env) -> Union[CollectionSelection, List[StrictStr]]:
        return self.__dict__[env]
