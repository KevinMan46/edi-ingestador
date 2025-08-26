import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER", "elastic")
    ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD", "tu_clave")
    TIKA_SERVER_URL = os.getenv("TIKA_SERVER_URL", "http://tika:9998")
    INDEX_NAME = "archivo_digital_edi"

settings = Settings()