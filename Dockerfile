# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Atualiza a lista de pacotes e instala as dependências necessárias
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    build-essential \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copia os arquivos de requisitos primeiro, para aproveitar o cache da camada
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt --verbose

# Copia o restante dos arquivos do seu projeto para o diretório de trabalho
COPY . .

# Expõe a porta em que o Flask será executado
EXPOSE 3000

# Define o comando para rodar a aplicação com Gunicorn
CMD ["sh", "-c", "gunicorn -w 4 -b 0.0.0.0:${PORT} buttons-interaction:app"]
