import fitz  # PyMuPDF
from tika import parser
import tika
import os
import tempfile
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.services.elasticsearch_service import ElasticsearchService

logger = setup_logger(__name__)

class PDFProcessor:
    def __init__(self):
        tika.TikaClientOnly = True
        self.tika_server_url = settings.TIKA_SERVER_URL
        #self.es = ElasticsearchService

    def process_pdf(self, pdf_path, file_name, expediente_id, cuaderno_id, documento_id, archivo_digital_id, nro_expediente, anio_expediente, es: ElasticsearchService):
        """
        Actualiza un documento en Elasticsearch por archivoDigitalId.

        :param es_client: instancia de Elasticsearch
        :param index_name: nombre del índice
        :param archivo_digital_id: valor de archivoDigitalId a buscar
        :param expediente_id: nuevo valor para expedienteId
        :param cuaderno_id: nuevo valor para cuadernoId
        :param documento_id: nuevo valor para documentoId
        :param nro_expediente: nuevo valor para nroExpediente
        :param anio_expediente: nuevo valor para anioExpediente
        :param contenido: lista de objetos con {"pagina": int, "texto": str}
        """
        try:
            logger.info(f"Processing PDF: {file_name}")
            response = parser.from_buffer("Test", self.tika_server_url)
            logger.info("Tika server connection successful")

            pdf = fitz.open(pdf_path)
            pages_processed = 0
            page_contents = []

            for page_num in range(pdf.page_count):
                page_pdf = fitz.open()
                page_pdf.insert_pdf(pdf, from_page=page_num, to_page=page_num)
                temp_pdf = f"temp_page_{page_num + 1}.pdf"
                page_pdf.save(temp_pdf)
                page_pdf.close()

                logger.info(f"Processing page {page_num + 1} with Tika")
                parsed = parser.from_file(temp_pdf, self.tika_server_url, xmlContent=False)
                content = parsed.get("content", "").strip()
                logger.info(f"Extracted content length: {len(content)}")

                if not content:
                    logger.info(f"Content empty, trying OCR for page {page_num + 1}")
                    parsed = parser.from_file(temp_pdf, self.tika_server_url, xmlContent=False, requestOptions={"X-Tika-PDFocrStrategy": "ocr_only"})
                    content = parsed.get("content", "").strip()
                    logger.info(f"OCR content length: {len(content)}")

                page_contents.append({"numeroPagina": page_num + 1, "texto": content})
                pages_processed += 1
                os.remove(temp_pdf)
            #es = ElasticsearchService
            exists = es.document_exists(archivo_digital_id)
            if exists == 1:
                doc = {
                    "query": {
                        "term": {
                            "archivoDigitalId": archivo_digital_id
                        }
                    },
                    "script": {
                        "source": "ctx._source.expedienteId = params.expedienteId; "
                        "ctx._source.cuadernoId = params.cuadernoId; "
                        "ctx._source.documentoId = params.documentoId; "
                        "ctx._source.nroExpediente = params.nroExpediente; "
                        "ctx._source.metadata = params.metadata; "
                        "ctx._source.anioExpediente = params.anioExpediente; "
                        "ctx._source.archivoDigital.contenido = params.contenido;",
                        "lang": "painless",
                        "params": {
                            "expedienteId": expediente_id,
                            "cuadernoId": cuaderno_id,
                            "documentoId": documento_id,
                            "nroExpediente": nro_expediente,
                            "metadata": parsed.get("metadata", {}),
                            "anioExpediente": anio_expediente,
                            "contenido": page_contents
                        }
                    }
                }
            else:
                #versión actual, ahora el índice es: "archivo_digital_edi" con la estructura:
                doc = {
                    "anioExpediente": anio_expediente,
                    "archivoDigitalId": archivo_digital_id,
                    "cuadernoId": cuaderno_id,
                    "documentoId": documento_id,
                    "expedienteId": expediente_id,
                    "numeroExpediente": nro_expediente,
                    "metadata": parsed.get("metadata", {}),
                    "archivoDigital": {
                        "rutaArchivoDigital": pdf_path,
                        "contenido": page_contents
                    },
                    "acciones": {}
                }


            pdf.close()
            return {
                "status": "success",
                "file_name": file_name,
                "pages_processed": pages_processed,
                "pages": page_contents,
                "message": "All file processed successfully",
                "exists": exists,
                "doc": doc
            }
            #return doc
        except Exception as e:
            logger.error(f"Error processing PDF: {str(e)}")
            return {
                "status": "failure",
                "file_name": file_name,
                "pages_processed": 0,
                "pages": [],
                "exists": -1,
                "message": str(e)
            }