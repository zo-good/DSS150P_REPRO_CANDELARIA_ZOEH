# Pipeline image: the same Python + packages on every machine
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# install dependencies first (this layer is cached until requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the code and configuration (data comes in through volumes, not the image)
COPY src/ src/
COPY config/ config/
COPY sql/ sql/
COPY metadata/ metadata/

CMD ["python", "-m", "src.cli", "--help"]