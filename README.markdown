# Document Processing API

A FastAPI-based application for processing PDF documents, extracting text using Tika, and indexing/searching content in Elasticsearch.

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables**:
   Create a `.env` file with the following:
   ```
   ELASTICSEARCH_URL=http://192.168.27.12:9200
   ELASTICSEARCH_USER=elastic
   ELASTICSEARCH_PASSWORD=tu_clave
   TIKA_SERVER_URL=http://tika:9998
   ```

3. **Run the application**:
   ```bash
   uvicorn src.main:app --host 0.0.0.0 --port 8500
   ```

## API Endpoints

- **POST /upload**: Upload and process a PDF file.
- **POST /_search**: Search for a keyword across all documents.
- **GET /search**: Search for a keyword across all documents (query parameter).
- **GET /search/{doc_id}**: Search for a keyword in a specific document.

## Project Structure

- `src/config/`: Configuration settings.
- `src/services/`: Business logic for Elasticsearch and PDF processing.
- `src/models/`: Pydantic models for request/response validation.
- `src/api/`: FastAPI route definitions.
- `src/utils/`: Utility functions (e.g., logging).
- `src/main.py`: Application entry point.