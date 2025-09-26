import fitz  # PyMuPDF
import tempfile
import os
from pathlib import Path
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Cargar variables desde .env
#load_dotenv()
PDF_BASE_DIR = Path(os.getenv("PDF_BASE_DIR", "/data/pdfs"))
SFTP_USER = os.getenv("FTP_USER")
SFTP_PASSWORD = os.getenv("FTP_PASSWORD")
SFTP_HOST = os.getenv("FTP_HOST")
SFTP_PORT = int(os.getenv("FTP_PORT", 22))
SFTP_DIR = os.getenv("FTP_DIR")

class  UtilsPDFMethods:   

    def getMetadata_LEGACY(self, pdf_path):
        try:
            pdf = fitz.open(pdf_path)
            metadata = pdf.metadata
            pdf.close()
            return metadata
        except Exception as e:
            logger.error(f"Error obteniendo metadata: {str(e)}")
            return {}

    def get_metadata(self, remote_pdf_path: str):
        """
        Extrae metadatos de un PDF ubicado en servidor SFTP.
        
        Args:
            remote_pdf_path (str): Ruta completa del PDF en el servidor SFTP
                                Ej: "/ruta/en/servidor/archivo.pdf" 
                                o solo "archivo.pdf" (se buscará en SFTP_DIR)
        
        Returns:
            dict: Diccionario con metadatos del PDF
        """
        # Si es solo el nombre del archivo, construir la ruta completa
        if not remote_pdf_path.startswith('/'):
            remote_pdf_path = f"{SFTP_DIR}/{remote_pdf_path}"
        
        # Crear archivo temporal local único
        local_temp_file = None
        
        try:
            self.connect()
            
            # 1. Verificar que el archivo existe en el servidor
            try:
                file_stats = self.sftp.stat(remote_pdf_path)
                remote_file_size = file_stats.st_size
            except FileNotFoundError:
                raise FileNotFoundError(f"❌ No se encontró el archivo remoto: {remote_pdf_path}")
            
            # 2. Crear archivo temporal local
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                local_temp_file = tmp_file.name
                
            # 3. Descargar archivo desde SFTP
            logger.info(f"Descargando {remote_pdf_path} para análisis de metadatos...")
            self.sftp.get(remote_pdf_path, local_temp_file)
            
            # 4. Extraer metadatos usando PyMuPDF
            with fitz.open(local_temp_file) as doc:
                metadata = doc.metadata  # Diccionario con metadatos internos
                num_pages = len(doc)
                
                # Información adicional del documento
                page_info = []
                for page_num in range(min(3, num_pages)):  # Solo primeras 3 páginas para performance
                    page = doc[page_num]
                    page_info.append({
                        "numero": page_num + 1,
                        "ancho_pts": page.rect.width,
                        "alto_pts": page.rect.height,
                        "rotacion": page.rotation
                    })
            
            # 5. Calcular tamaño en MB
            file_size_mb = remote_file_size / (1024 * 1024)
            
            # 6. Extraer nombre del archivo de la ruta
            file_name = os.path.basename(remote_pdf_path)
            
            # 7. Armar resultado completo
            result = {
                "nombre_archivo": file_name,
                "ruta_remota": remote_pdf_path,
                "peso_bytes": remote_file_size,
                "peso_mb": round(file_size_mb, 2),
                "paginas": num_pages,
                "metadatos_pdf": metadata,
                "info_paginas_muestra": page_info,
                "servidor": {
                    "host": SFTP_HOST,
                    "directorio": os.path.dirname(remote_pdf_path)
                }
            }
            
            logger.info(f"✅ Metadatos extraídos: {file_name} ({num_pages} páginas, {file_size_mb:.2f} MB)")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo metadatos de {remote_pdf_path}: {str(e)}")
            raise
            
        finally:
            # Limpiar archivo temporal
            if local_temp_file and os.path.exists(local_temp_file):
                try:
                    os.unlink(local_temp_file)
                    logger.debug(f"🗑️ Archivo temporal eliminado: {local_temp_file}")
                except Exception as e:
                    logger.warning(f"⚠️ No se pudo eliminar archivo temporal {local_temp_file}: {e}")
            
            self.disconnect()
