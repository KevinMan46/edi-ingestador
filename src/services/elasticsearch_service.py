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

    from elasticsearch import Elasticsearch

    def update_document(
        self,
        archivo_digital_id: str,
        expediente_id: str,
        cuaderno_id: str,
        documento_id: str,
        nro_expediente: str,
        anio_expediente: int,
        contenido: list
    ):
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
        body = {
            "query": {
                "term": {  # usar term en lugar de match para keyword exacto
                    "archivoDigitalId.keyword": archivo_digital_id
                }
            },
            "script": {
                "source": (
                    "ctx._source.expedienteId = params.expedienteId; "
                    "ctx._source.cuadernoId = params.cuadernoId; "
                    "ctx._source.documentoId = params.documentoId; "
                    "ctx._source.nroExpediente = params.nroExpediente; "
                    "ctx._source.anioExpediente = params.anioExpediente; "
                    "ctx._source.archivoDigital.contenido = params.contenido;"
                ),
                "lang": "painless",
                "params": {
                    "expedienteId": expediente_id,
                    "cuadernoId": cuaderno_id,
                    "documentoId": documento_id,
                    "nroExpediente": nro_expediente,
                    "anioExpediente": anio_expediente,
                    "contenido": contenido
                }
            }
        }

        response = self.es.update_by_query(index=self.index_name, body=body)
        return response

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
            return response["hits"]["total"]["value"] > 0
            
        except Exception as e:
            print(f"Error al verificar el documento: {e}")
            return False
    
    def search(self, query):
        try:
            response = self.es.search(index=self.index_name, body=query)
            return response
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise