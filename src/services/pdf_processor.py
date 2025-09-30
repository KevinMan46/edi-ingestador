import fitz  # PyMuPDF
from tika import parser
import tika
import tempfile
import paramiko
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.services.elasticsearch_service import ElasticsearchService
from pathlib import Path
import stat
from ftplib import FTP, error_perm
import math, os, shutil

logger = setup_logger(__name__)


# Cargar variables desde .env
#load_dotenv()
PDF_BASE_DIR = Path(os.getenv("PDF_BASE_DIR", "/data/pdfs"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_DIR = os.getenv("SFTP_DIR")
PDF_BASE_DIR = os.getenv("PDF_BASE_DIR")

FTP_USER = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")
FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", 21))

class PDFProcessor:
    def __init__(self):
        tika.TikaClientOnly = True
        self.tika_server_url = settings.TIKA_SERVER_URL
        #self.es = ElasticsearchService
        self.transport = None
        self.sftp = None

    def process_pdf(self, pdf_path, file_name, expediente_id, cuaderno_id, documento_id, archivo_digital_id, nro_expediente, anio_expediente, documento_nombre, es: ElasticsearchService):
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
        # Configurar opciones para forzar OCR
        request_options = {
            "headers": {
                "X-Tika-PDFocrStrategy": "ocr_and_text",
                "X-Tika-OCRLanguage": "spa"
            }
        }
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

                #opción solo con TXTS PURO Ó ESCANEADO
                parsed = parser.from_file(temp_pdf, self.tika_server_url, xmlContent=False) #originalmente estaba así

                #opción para que tomo todo dentro de una página (fotos incrustadas y escaneado (híbrido)), esto es más completo, pero demasiado lento y a veces duplica el texto de una hoja
                #parsed = parser.from_file(temp_pdf, self.tika_server_url, requestOptions=request_options, xmlContent=False)

                content = (parsed.get("content", "") or "").strip()
                logger.info(f"Extracted content length: {len(content)}")

                # if not content:
                #     logger.info(f"Content empty, trying OCR for page {page_num + 1}")
                #     parsed = parser.from_file(temp_pdf, self.tika_server_url, xmlContent=False, requestOptions={"X-Tika-PDFocrStrategy": "ocr_only"})
                #     content = parsed.get("content", "").strip()
                #     logger.info(f"OCR content length: {len(content)}")

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
                        "ctx._source.documentoNombre = params.documentoNombre; "
                        "ctx._source.archivoDigital.contenido = params.contenido;",
                        "lang": "painless",
                        "params": {
                            "expedienteId": expediente_id,
                            "cuadernoId": cuaderno_id,
                            "documentoId": documento_id,
                            "nroExpediente": nro_expediente,
                            "metadata": parsed.get("metadata", {}),
                            "anioExpediente": anio_expediente,
                            "documentoNombre": documento_nombre,
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
                    "documentoNombre": documento_nombre,
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
                "pages": page_contents,
                "message": "All file processed successfully",
                "exists": exists,
                "pages_processed": pages_processed,
                "doc": doc
            }
            #return doc
        except Exception as e:
            logger.error(f"Error processing PDF: {str(e)}")
            return {
                "status": "failure",
                "file_name": file_name,
                "pages_processed": 0,
                "exists": -1,
                "pages": [],
                "message": "Error processing PDF: "+str(e)
            }
        

    def split_pdf(self, input_pdf: str, out_dir: str, chunk_size: int = 1000):
        input_path = Path(input_pdf)
        output_dir = Path(out_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        with fitz.open(str(input_path)) as doc:
            total_pages = len(doc)
            n_parts = math.ceil(total_pages / chunk_size)
            indice_path = output_dir / "indice.txt"

            with open(indice_path, "w", encoding="utf-8") as idx:
                for i in range(n_parts):
                    start = i * chunk_size
                    end = min(start + chunk_size, total_pages) - 1

                    out_file = output_dir / f"{input_path.stem}_part{i+1:05d}.pdf"
                    new_doc = fitz.open()  # doc vacío

                    new_doc.insert_pdf(doc, from_page=start, to_page=end)
                    new_doc.save(out_file, deflate=False, garbage=0)
                    new_doc.close()

                    idx.write(f"{out_file.name} [{start+1}-{end+1}]\n")

        return {
            "total_pages": total_pages,
            "parts": n_parts,
            "index_file": str(indice_path),
            "output_dir": str(output_dir)
        }
    
    def split_pdf_v2(self, input_pdf: str, chunk_size: int = 1000):
        input_path = Path(input_pdf)

        # Si solo pasaron el nombre del archivo → lo buscamos en la carpeta base
        if not input_path.is_absolute():
            input_path = PDF_BASE_DIR / input_path

        if not input_path.exists():
            raise FileNotFoundError(f"El archivo {input_path} no existe")

        # Crear carpeta de salida al lado del archivo original
        output_dir = input_path.parent / f"{input_path.stem}_parts"
        output_dir.mkdir(parents=True, exist_ok=True)

        with fitz.open(str(input_path)) as doc:
            total_pages = len(doc)
            n_parts = math.ceil(total_pages / chunk_size)
            indice_path = output_dir / "indice.txt"

            with open(indice_path, "w", encoding="utf-8") as idx:
                for i in range(n_parts):
                    start = i * chunk_size
                    end = min(start + chunk_size, total_pages) - 1
                    out_file = output_dir / f"{input_path.stem}_part{i+1:05d}.pdf"

                    new_doc = fitz.open()
                    new_doc.insert_pdf(doc, from_page=start, to_page=end)
                    new_doc.save(out_file, deflate=False, garbage=0)
                    new_doc.close()

                    idx.write(f"{out_file.name} [{start+1}-{end+1}]\n")

        return {
            "total_pages": total_pages,
            "parts": n_parts,
            "index_file": str(indice_path),
            "output_dir": str(output_dir)
        }
    
    #METODO FTP

    def connect(self):
        """Establecer conexión SFTP usando paramiko"""
        self.transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        self.transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
    
    def disconnect(self):
        """Cerrar conexión SFTP"""
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        
    #Método observado, no borrra algunos archivos, por eso se creó el otro método _clean_dir debajo
    def _rmdir_recursive(self, path: str):
        """Eliminar recursivamente un directorio remoto en SFTP (simula rm -rf)."""
        try:
            while True:
                files = self.sftp.listdir(path)
                if not files:
                    break
                for filename in files:
                    item_path = f"{path}/{filename}"
                    try:
                        self.sftp.remove(item_path)   # si es archivo
                    except IOError:
                        self._rmdir_recursive(item_path)  # si es carpeta
            self.sftp.rmdir(path)
        except FileNotFoundError:
            pass

    def _ftp_rmdir_recursive(self, ftp: FTP, path: str):
        """Eliminar recursivamente un directorio en FTP simple."""
        try:
            ftp.cwd(path)
        except error_perm:
            return

        items = []
        ftp.retrlines("LIST", items.append)

        for item in items:
            parts = item.split()
            name = parts[-1]
            if name in (".", ".."):
                continue

            full_path = f"{path}/{name}"

            if item.upper().startswith("D"):  # Es un directorio
                self._ftp_rmdir_recursive(ftp, full_path)
                try:
                    ftp.rmd(full_path)
                except:
                    pass
            else:  # Es un archivo
                try:
                    ftp.delete(full_path)
                except:
                    pass

    def _clean_dir(self, path):
            """Borra todos los contenidos de un directorio remoto (archivos y subcarpetas)."""
            try:
                for item in self.sftp.listdir_attr(path):  # Use listdir_attr for consistency and mode check
                    if item.filename in ('.', '..'):
                        continue
                    item_path = f"{path}/{item.filename}"
                    if stat.S_ISDIR(item.st_mode):
                        self._rmdir_recursive(item_path)  # Recurse for subdir
                    else:
                        try:
                            self.sftp.remove(item_path)
                        except Exception as e:
                            logger.error(f"No se pudo eliminar archivo {item_path}: {e}")
            except FileNotFoundError:
                return
            except Exception as e:
                logger.error(f"Error al limpiar {path}: {e}")
    
    def _delete_all(self, path):
        """Borra todos los contenidos de un directorio remoto (archivos dentro)."""
        try:
            for item in self.sftp.listdir_attr(path):  # Use listdir_attr for consistency and mode check
                logger.info(f"KASCA==Deleting item: {item.filename}")
                item_path = f"{path}/{item.filename}"
                try:
                    self.sftp.remove(item_path)
                except Exception as e:
                    logger.error(f"No se pudo eliminar archivo {item_path}: {e}")
        except FileNotFoundError:
            return
        except Exception as e:
            logger.error(f"Error al limpiar {path}: {e}")

    def split_pdf_sftp(self, input_pdf: str, chunk_size: int = 1000):
        local_tmp = Path("/tmp") / input_pdf
        local_tmp.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.connect()

            remote_path = f"{SFTP_DIR}/{input_pdf}"

            # 1. Verificar archivo en remoto
            try:
                self.sftp.stat(remote_path)
            except FileNotFoundError:
                raise FileNotFoundError(f"El archivo remoto {remote_path} no existe")

            # 2. Descargar archivo
            self.sftp.get(remote_path, str(local_tmp))

            # 3. Procesar localmente
            output_dir = local_tmp.parent / f"{local_tmp.stem}_parts"
            output_dir.mkdir(parents=True, exist_ok=True)

            # 4. Procesar PDF
            with fitz.open(str(local_tmp)) as doc:
                total_pages = len(doc)
                n_parts = math.ceil(total_pages / chunk_size)
                indice_path = output_dir / "indice.txt"

                with open(indice_path, "w", encoding="utf-8") as idx:
                    for i in range(n_parts):
                        start = i * chunk_size
                        end = min(start + chunk_size, total_pages) - 1
                        out_file = output_dir / f"{local_tmp.stem}_part{i+1:05d}.pdf"

                        new_doc = fitz.open()
                        new_doc.insert_pdf(doc, from_page=start, to_page=end)
                        new_doc.save(out_file, deflate=False, garbage=0)
                        new_doc.close()

                        idx.write(f"{out_file.name} [{start+1}-{end+1}]\n")

            # 5. Subir resultados a la carpeta remota
            remote_output = f"{os.path.dirname(remote_path)}/{local_tmp.stem}_parts"
            # 🔑 Fuerza reconexión antes de intentar borrar
            self.disconnect()
            self.connect()

            # Si existe, limpiar contenidos y luego eliminar la carpeta
            try:
                self.sftp.stat(remote_output)
                self._rmdir_recursive(remote_output)
                #self._delete_all(remote_output)
            except FileNotFoundError:
                pass

            # Crear de nuevo la carpeta
            self.sftp.mkdir(remote_output)

            # Subir los archivos nuevos
            for file in output_dir.iterdir():
                self.sftp.put(str(file), f"{remote_output}/{file.name}")


            return {
                "total_pages": total_pages,
                "parts": n_parts,
                "remote_output": remote_output,
                "index_file": f"{remote_output}/indice.txt",
            }

        finally:
            # Limpiar archivos temporales locales
            try:
                if local_tmp.exists():
                    local_tmp.unlink()
                if output_dir.exists():
                    import shutil
                    shutil.rmtree(output_dir)
            except Exception as e:
                logger.warning(f"Error limpiando archivos temporales: {e}")
            self.disconnect()


    def split_pdf_ftp(self, input_pdf: str, chunk_size: int = 1000):
        local_tmp = Path("/tmp") / Path(input_pdf).name
        local_tmp.parent.mkdir(parents=True, exist_ok=True)

        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASSWORD)

        try:
            # 1. Detectar carpeta y nombre del archivo remoto
            remote_dir = os.path.dirname(input_pdf) or "."
            remote_file = os.path.basename(input_pdf)

            # 2. Descargar archivo original desde el FTP
            ftp.cwd(remote_dir)
            with open(local_tmp, "wb") as f:
                ftp.retrbinary(f"RETR " + remote_file, f.write)

            # 3. Procesar localmente en partes
            output_dir = local_tmp.parent / f"{local_tmp.stem}_parts"
            output_dir.mkdir(parents=True, exist_ok=True)

            with fitz.open(str(local_tmp)) as doc:
                total_pages = len(doc)
                n_parts = math.ceil(total_pages / chunk_size)
                indice_path = output_dir / "indice.txt"

                with open(indice_path, "w", encoding="utf-8") as idx:
                    for i in range(n_parts):
                        start = i * chunk_size
                        end = min(start + chunk_size, total_pages) - 1
                        out_file = output_dir / f"{local_tmp.stem}_part{i+1:05d}.pdf"

                        new_doc = fitz.open()
                        new_doc.insert_pdf(doc, from_page=start, to_page=end)
                        new_doc.save(out_file, deflate=False, garbage=0)
                        new_doc.close()

                        idx.write(f"{out_file.name} [{start+1}-{end+1}]\n")

            # 4. Crear carpeta remota limpia justo al lado del PDF original
            remote_output = f"{remote_dir}/{local_tmp.stem}_parts"

            # Si ya existe, eliminarla recursivamente
            try:
                self._ftp_rmdir_recursive(ftp, remote_output)
                ftp.rmd(remote_output)
            except:
                pass

            # Crear carpeta nueva
            ftp.mkd(remote_output)

            # 5. Subir partes y el índice al FTP
            ftp.cwd(remote_output)
            for file in output_dir.iterdir():
                with open(file, "rb") as f:
                    ftp.storbinary(f"STOR " + file.name, f)

            return {
                "total_pages": total_pages,
                "parts": n_parts,
                "remote_output": remote_output,
                "index_file": f"{remote_output}/indice.txt"
            }

        finally:
            ftp.quit()
            # Limpiar temporales locales
            try:
                if local_tmp.exists():
                    local_tmp.unlink()
                if output_dir.exists():
                    shutil.rmtree(output_dir)
            except:
                pass