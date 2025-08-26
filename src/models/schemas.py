from pydantic import BaseModel
from typing import List

class SearchRequest(BaseModel):
    keyword: str

class SearchResult(BaseModel):
    file_name: str
    page_number: int
    content_snippet: str