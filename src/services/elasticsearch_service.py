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
        mapping = {
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "properties": {
                    "anotacion": {
                        "properties": {
                            "anotacionId": {"type": "keyword"},
                            "archivoDigitalHijos": {
                                "type": "nested",
                                "properties": {
                                    "archivoDigitalId": {"type": "keyword"},
                                    "conversionArchivoDigital": {
                                        "type": "nested",
                                        "properties": {
                                            "numeroPagina": {"type": "integer"},
                                            "texto": {"type": "text"}
                                        }
                                    },
                                    "nombreArchivoDigital": {"type": "keyword"},
                                    "nombreOriginalArchivoDigital": {"type": "keyword"},
                                    "rutaArchivoDigital": {"type": "keyword"}
                                }
                            },
                            "codigoUsuario": {"type": "keyword"},
                            "color": {"type": "keyword"},
                            "fechaRegistro": {"type": "date"},
                            "marcaTiempo": {"type": "keyword"},
                            "nroPaginaArchivoDig": {"type": "keyword"},
                            "palabrasClave": {"type": "text"},
                            "posicionFin": {"type": "keyword"},
                            "posicionIni": {"type": "keyword"},
                            "tema": {"type": "text"},
                            "texto": {"type": "text"},
                            "textoCoordenada": {"type": "keyword"},
                            "tipoAnotacion": {"type": "keyword"},
                            "titulo": {"type": "text"}
                        }
                    },
                    "archivoDigitalPadre": {
                        "properties": {
                            "archivoDigitalId": {"type": "keyword"},
                            "conversionArchivoDigital": {
                                "type": "nested",
                                "properties": {
                                    "numeroPagina": {"type": "integer"},
                                    "texto": {"type": "text"}
                                }
                            },
                            "nombreArchivoDigital": {"type": "keyword"},
                            "nombreOriginalArchivoDigital": {"type": "keyword"},
                            "rutaArchivoDigital": {"type": "keyword"}
                        }
                    },
                    "conversionDocumento": {"type": "text"},
                    "documentoId": {"type": "keyword"},
                    "flagActivo": {"type": "keyword"},
                    "textoCompleto": {"type": "text"},
                    "expedienteId": {"type": "integer"},
                    "numeroExpediente": {"type": "keyword"},
                    "anioExpediente": {"type": "integer"},
                    "cuadernoId": {"type": "integer"},
                    "docId": {"type": "integer"},
                    "metadata": {"type": "object", "enabled": True},
                }
            }
        }
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

    def search(self, query):
        try:
            response = self.es.search(index=self.index_name, body=query)
            return response
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise