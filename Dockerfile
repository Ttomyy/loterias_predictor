# ==========================
# Imagen base ligera de Python
# ==========================
FROM python:3.11-slim

# ==========================
# Variables de entorno
# ==========================
# Evitar que Python cree archivos .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ==========================
# Carpeta de trabajo dentro del contenedor
# ==========================
WORKDIR /app

# ==========================
# Copiar solo archivos necesarios
# ==========================
COPY requirements.txt .
COPY run_all_Siniteraccion.sh .
COPY src/ ./src/   # Si tu código está en la carpeta src
# Ajusta según tu estructura de proyecto

# ==========================
# Instalar dependencias
# ==========================
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

# ==========================
# Dar permisos de ejecución al script
# ==========================
RUN chmod +x run_all_Siniteraccion.sh

# ==========================
# Comando por defecto con parámetro opcional
# ==========================
ENTRYPOINT ["bash", "run_all_Siniteraccion.sh"]
CMD ["bonoloto"]

