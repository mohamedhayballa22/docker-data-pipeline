from pydantic import BaseModel
from typing import List

class DataItem(BaseModel):
    id: int
    title: str
    wordCount: int
    summary: str

class DataResponse(BaseModel):
    items: List[DataItem]
    count: int