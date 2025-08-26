from fastapi import FastAPI
from src.services.elasticsearch_service import ElasticsearchService
from src.services.pdf_processor import PDFProcessor
from src.api.routes import setup_routes

app = FastAPI()
es_service = ElasticsearchService()
pdf_processor = PDFProcessor()

setup_routes(app, es_service, pdf_processor)