from elasticsearch import Elasticsearch
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ElasticsearchService:
    def __init__(self):
        self.es = Elasticsearch(
            [settings.ELASTICSEARCH_URL],
            http_auth=(settings.ELASTICSEARCH_USER, settings.ELASTICSEARCH_PASSWORD)
        )
        self.index_name = settings.INDEX_NAME

    def create_index(self):
        mapping = {}
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=mapping)
            logger.info(f"Created Elasticsearch index: {self.index_name}")

    def index_document(self, doc):
        try:
            self.es.index(index=self.index_name, body=doc)
            logger.info("Document indexed successfully")
        except Exception as e:
            logger.error(f"Error indexing document: {str(e)}")
            raise
    def update_document(self, doc):
        try:
            response = self.es.update_by_query(index=self.index_name, body=doc, conflicts="proceed")
            #logger.info("Document updated successfully")
            #return response
            logger.info(response)
            return response
        except Exception as e:
            logger.error(f"Error updating document: {str(e)}")
            raise

    def document_exists(self, archivo_digital_id):
        """
        Verifica si existe un documento con el archivoDigitalId especificado en el índice dado.
        
        Args:
            archivo_digital_id (str): El valor de archivoDigitalId a buscar (ej. "3130").
            index (str): El nombre del índice en Elasticsearch (por defecto: "archivo_digital_edi").
        
        Returns:
            bool: True si existe al menos un documento con el archivoDigitalId, False si no.
        """
        try:            
            # Realizar la búsqueda
            response = self.es.search(
                index=self.index_name,
                body={
                    "query": {
                        "term": {
                            "archivoDigitalId": archivo_digital_id
                        }
                    },
                    "size": 1  # Limitar a 1 resultado para optimizar
                }
            )
            
            # Verificar si hay documentos en los resultados
            if response["hits"]["total"]["value"] > 0:
                return 1
            else:
                return 0
            
        except Exception as e:
            print(f"Error al verificar el documento: {e}")
            return -1
    
    def search(self, query):
        try:
            response = self.es.search(index=self.index_name, body=query)
            return response
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise