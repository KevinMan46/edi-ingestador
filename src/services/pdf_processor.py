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
        self.es = ElasticsearchService

    def process_pdf(self, pdf_path, file_name, expediente_id, cuaderno_id, documento_id, archivo_digital_id, nro_expediente, anio_expediente):
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

            #antigua versión, cuando el índice era: anotaciones_archivo 
            # doc_legacy = {
            #     "anotacion": {
            #         "anotacionId": "123",
            #         "archivoDigitalHijos": [
            #             {
            #                 "archivoDigitalId": documento_id,
            #                 "conversionArchivoDigital": [],
            #                 "nombreArchivoDigital": "file1.pdf",
            #                 "nombreOriginalArchivoDigital": "original1.pdf",
            #                 "rutaArchivoDigital": "/path/to/demopath"
            #             }
            #         ],
            #         "codigoUsuario": "user1",
            #         "color": "blue",
            #         "fechaRegistro": "2025-08-12",
            #         "marcaTiempo": "2025-08-12T14:34:00",
            #         "nroPaginaArchivoDig": str(pages_processed),
            #         "palabrasClave": "clave1 clave2",
            #         "posicionFin": "100",
            #         "posicionIni": "0",
            #         "tema": "1",
            #         "texto": "texto de la anotación",
            #         "textoCoordenada": "x:10,y:20",
            #         "tipoAnotacion": "nota",
            #         "titulo": "Título de la anotación"
            #     },
            #     "archivoDigitalPadre": {
            #         "archivoDigitalId": documento_id,
            #         "conversionArchivoDigital": page_contents,
            #         "nombreArchivoDigital": file_name,
            #         "nombreOriginalArchivoDigital": file_name,
            #         "rutaArchivoDigital": pdf_path
            #     },
            #     "conversionDocumento": "texto del documento-de ejemplo",
            #     "documentoId": documento_id,
            #     "flagActivo": "true",
            #     "textoCompleto": "texto completo del documento-de ejemplo",
            #     "expedienteId": expediente_id,
            #     "numeroExpediente": nro_expediente,
            #     "anioExpediente": anio_expediente,
            #     "cuadernoId": cuaderno_id,
            #     "metadata": parsed.get("metadata", {}),
            # }

            #versión actual, ahora el índice es: "archivo_digital_edi" con la estructura:
            exists = self.es.document_exists(archivo_digital_id)
            if exists:
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
            else:
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
                "pages_processed": "NNNNNNN",
                "pages": [],
                "message": str(e)
            }