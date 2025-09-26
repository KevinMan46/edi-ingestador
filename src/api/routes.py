from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from src.services.elasticsearch_service import ElasticsearchService
from src.services.pdf_processor import PDFProcessor
from src.models.schemas import SearchRequest, SearchResult
from src.utils.logger import setup_logger
from typing import List
import tempfile
import os
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

logger = setup_logger(__name__)

# -------- Modelos --------
class SplitRequest(BaseModel):
    filename: str
    chunk_size: int = 1000

def setup_routes(app: FastAPI, es_service: ElasticsearchService, pdf_processor: PDFProcessor):
    @app.on_event("startup")
    async def startup_event():
        es_service.create_index()

    @app.post("/upload")
    async def upload_pdf(
        file: UploadFile = File(...),
        file_name: str = Form(None),
        expediente_id: int = Form(0),
        cuaderno_id: int = Form(0),
        documento_id: int = Form(6),
        archivo_digital_id: int = Form(6),
        nro_expediente: str = Form("EXP-XYZZZZZ"),
        documento_nombre: str = Form("Documento Ejemplo"),
        anio_expediente: int = Form(2025)
    ):
        if not file.filename.lower().endswith(".pdf"):
            return JSONResponse(
                status_code=400,
                content={
                    "status": "failure",
                    "message": "Only PDF files are allowed!",
                    "file_name": file.filename,
                    "existence": -1,
                    "pages_processed": 0,
                    "pages": []

                    
                }
            )

        final_file_name = file_name if file_name else file.filename
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(await file.read())
            temp_file_path = temp_file.name

        try:
            result = pdf_processor.process_pdf(
                temp_file_path, final_file_name, expediente_id, cuaderno_id,
                documento_id, archivo_digital_id, nro_expediente, anio_expediente, documento_nombre, es_service
            )
            if result["status"] == "success":
                if result["exists"] == 1:
                    es_service.update_document(result["doc"])
                else:
                    es_service.index_document(result["doc"])
                return {
                    "status": result["status"],
                    "message": result["message"],
                    "file_name": result["file_name"],
                    "existence": result["exists"],
                    "pages_processed": result["pages_processed"],
                    "pages": result["pages"]
                }
            else:
                return JSONResponse(
                    status_code=400,
                    content={
                        "status": "failure",
                        "message": result["message"],
                        "file_name": result["file_name"],
                        "existence": result["exists"],
                        "pages_processed": result["pages_processed"],
                        "pages": result["pages"]
                        
                    }
                )
        finally:
            os.unlink(temp_file_path)

    @app.post("/split-pdf")
    async def split_pdf_endpoint(
        input_pdf: str = Form(...),
        chunk_size: int = Form(1000)
    ):
        try:
            result = await run_in_threadpool(pdf_processor.split_pdf_v2, input_pdf, chunk_size)
            return JSONResponse(content={
                "status": "success",
                "message": "PDF split completed",
                **result
            })
        except FileNotFoundError as e:
            return JSONResponse(status_code=404, content={
                "status": "failure",
                "message": str(e)
            })
        except Exception as e:
            return JSONResponse(status_code=500, content={
                "status": "failure",
                "message": str(e)
            })
    
    @app.post("/split-pdfs")
    def split_pdf_endpoint_v2(input_pdf: str = Form(...), chunk_size: int = Form(1000)):
        result = pdf_processor.split_pdf_v2(input_pdf, chunk_size)
        return result
    
    @app.post("/split-pdf-ftp")
    def split_pdf(req: SplitRequest):
        splitter = PDFProcessor()
        result = splitter.split_pdf_ftp(req.filename, req.chunk_size)
        return result