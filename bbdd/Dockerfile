# Utiliza una imagen base de Python 3.11
FROM python:3.11.4

# Configura el directorio de trabajo
WORKDIR /app

# Copia el archivo requirements.txt a la imagen
COPY requirements.txt .

# Instalar las dependencias y limpiar
RUN pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copiar los scripts
COPY fill_data_script2.py .
COPY scoring.py .
COPY autorechazo.py .

# Establecer el comando predeterminado
CMD ["bash", "-c", "python fill_data_script2.py && python autorechazo.py && python scoring.py "]
