# Use a lightweight Python base image
#FROM python:3.11-slim # SE USABA ANTES ESTA IMAGEN, PERO AHORA NO FUNCIONA

#INICIO DE BLOQUE DE PYTHON 3.11
# Imagen base desde el mirror de Google
FROM mirror.gcr.io/ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl git build-essential \
    libffi-dev libssl-dev zlib1g-dev libpq-dev \
    libxml2-dev libxslt1-dev gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Agregar repo de Deadsnakes manualmente (sin add-apt-repository)
RUN mkdir -p /etc/apt/keyrings \
    && wget -qO- https://keyserver.ubuntu.com/pks/lookup?op=get\&search=0xF23C5A6CF475977595C89F51BA6932366A755776 \
       | gpg --dearmor > /etc/apt/keyrings/deadsnakes.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/deadsnakes.gpg] http://ppa.launchpad.net/deadsnakes/ppa/ubuntu jammy main" \
       > /etc/apt/sources.list.d/deadsnakes.list \
    && apt-get update \
    && apt-get install -y python3.11 python3.11-dev python3.11-distutils python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Configurar python3.11 como predeterminado
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && python3 -m pip install --upgrade pip
#FIN DE BLOQUE DE PYTHON 3.11

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project directory
COPY src/ ./src/
COPY .env .

# Expose the FastAPI port
EXPOSE 8500

# Command to run the app with Uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]