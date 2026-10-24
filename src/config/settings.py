import os
from dataclasses import dataclass

@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "novarota"
    schema_bronze: str = "bronze"
    schema_silver: str = "silver"
    schema_gold: str = "gold"
    
    @property
    def raw_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/stg/raw"
        
    @property
    def checkpoint_base(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/stg/_checkpoints"