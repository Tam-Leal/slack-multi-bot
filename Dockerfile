# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de requisitos primeiro, para aproveitar o cache da camada
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos do seu projeto para o diretório de trabalho
COPY . .

# Expõe a porta em que o Flask será executado
EXPOSE 3000

# Define o comando para rodar a aplicação
CMD ["python", "buttons-interaction.py"]
