from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse
from src.services.elasticsearch_service import ElasticsearchService
from src.services.pdf_processor import PDFProcessor
from src.models.schemas import SearchRequest, SearchResult
from src.utils.logger import setup_logger
from typing import List
import tempfile
import os

logger = setup_logger(__name__)

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
        anio_expediente: int = Form(2025)
    ):
        if not file.filename.lower().endswith(".pdf"):
            #definir acá el resultado JSON de error
            # raise HTTPException(
            #     status_code=400, 
            #     detail={
            #         "status": "failure",
            #         # "file_name": file.filename,
            #         # "pages_processed": 0,
            #         # "pages": [],
            #         "message": "Only PDF files are allowed ouououou...",
            #     }
            # )
            return JSONResponse(
                status_code=400,
                content={
                    "status": "failure",
                    "pages_processed": 0,
                    "pages": [],
                    "message": "Only PDF files are allowed!",
                    "file_name": file.filename,
                    
                }
            )

        final_file_name = file_name if file_name else file.filename
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(await file.read())
            temp_file_path = temp_file.name

        try:
            result = pdf_processor.process_pdf(
                temp_file_path, final_file_name, expediente_id, cuaderno_id,
                documento_id, archivo_digital_id, nro_expediente, anio_expediente, es_service
            )
            if result["status"] == "success":
                if result["exists"] == "1":
                    es_service.update_document(result["doc"])
                else:
                    es_service.index_document(result["doc"])
            return {
                "status": "success",
                "pages_processed": result["pages_processed"],
                "pages": result["pages"],
                "message": result["message"],
                "file_name": result["file_name"],
                "existence": result["exists"]
            }
        finally:
            os.unlink(temp_file_path)

    @app.post("/_search")
    async def search_keyword(request: SearchRequest):
        query = {
            "query": {
                "nested": {
                    "path": "archivoDigitalPadre.conversionArchivoDigital",
                    "query": {
                        "match": {
                            "archivoDigitalPadre.conversionArchivoDigital.texto": request.keyword
                        }
                    }
                }
            },
            "highlight": {
                "fields": {
                    "archivoDigitalPadre.conversionArchivoDigital.texto": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            }
        }
        try:
            response = es_service.search(query)
            results = [
                {
                    "file_name": hit["_source"]["archivoDigitalPadre"]["nombreArchivoDigital"],
                    "page_number": page["numeroPagina"],
                    "content_snippet": snippet
                }
                for hit in response["hits"]["hits"]
                for page in hit["_source"]["archivoDigitalPadre"]["conversionArchivoDigital"]
                for snippet in hit.get("highlight", {}).get("archivoDigitalPadre.conversionArchivoDigital.texto", [""])
            ]
            logger.info(f"Found {len(results)} results for {request.keyword}")
            return {"results": results}
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

    @app.get("/search")
    async def search_keyword(keyword: str):
        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")
        query = {
            "query": {
                "nested": {
                    "path": "archivoDigitalPadre.conversionArchivoDigital",
                    "query": {
                        "match": {
                            "archivoDigitalPadre.conversionArchivoDigital.texto": keyword
                        }
                    }
                }
            },
            "highlight": {
                "fields": {
                    "archivoDigitalPadre.conversionArchivoDigital.texto": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            }
        }
        try:
            response = es_service.search(query)
            results = [
                {
                    "file_name": hit["_source"]["archivoDigitalPadre"]["nombreArchivoDigital"],
                    "page_number": page["numeroPagina"],
                    "content_snippet": snippet
                }
                for hit in response["hits"]["hits"]
                for page in hit["_source"]["archivoDigitalPadre"]["conversionArchivoDigital"]
                for snippet in hit.get("highlight", {}).get("archivoDigitalPadre.conversionArchivoDigital.texto", [""])
            ]
            logger.info(f"Found {len(results)} results for {keyword}")
            return {"results": results}
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

    @app.get("/search/{doc_id}", response_model=List[SearchResult])
    async def search_in_pdf(doc_id: int, query: str):
        if not query:
            raise HTTPException(status_code=400, detail="El parámetro 'query' no puede estar vacío")
        search_body = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"docId": doc_id}},
                        {
                            "bool": {
                                "should": [
                                    {"wildcard": {"archivoDigitalPadre.nombreArchivoDigital": "*.pdf"}},
                                    {"wildcard": {"archivoDigitalPadre.nombreOriginalArchivoDigital": "*.pdf"}}
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ],
                    "must": [
                        {
                            "nested": {
                                "path": "archivoDigitalPadre.conversionArchivoDigital",
                                "query": {
                                    "match": {
                                        "archivoDigitalPadre.conversionArchivoDigital.texto": query
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "archivoDigitalPadre.conversionArchivoDigital.texto": {
                        "fragment_size": 100,
                        "number_of_fragments": 1,
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"]
                    }
                }
            }
        }
        try:
            response = es_service.search(search_body)
            results = [
                {
                    "file_name": hit["_source"]["archivoDigitalPadre"].get("nombreArchivoDigital", hit["_source"]["archivoDigitalPadre"].get("nombreOriginalArchivoDigital", "unknown.pdf")),
                    "page_number": page["numeroPagina"],
                    "content_snippet": snippet
                }
                for hit in response["hits"]["hits"]
                for page in hit["_source"]["archivoDigitalPadre"]["conversionArchivoDigital"]
                for snippet in hit.get("highlight", {}).get("archivoDigitalPadre.conversionArchivoDigital.texto", [""])
            ]
            return results
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")