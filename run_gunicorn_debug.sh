#!/bin/bash

# Script para iniciar o Gunicorn com configurações para diagnóstico

# Criar diretórios necessários
mkdir -p envio_hsm_matrix/logs
mkdir -p envio_hsm_matrix/temp_csv
mkdir -p envio_hsm_matrix/cache

# Ajustar permissões
chmod -R 755 envio_hsm_matrix/logs
chmod -R 755 envio_hsm_matrix/temp_csv
chmod -R 755 envio_hsm_matrix/cache
chmod -R 755 envio_hsm_matrix/templates

# Verificar o ambiente antes de iniciar
echo "Verificando ambiente..."
python3 envio_hsm_matrix/check_gunicorn.py

# Definir variável de ambiente para debug
export FLASK_ENV=development
export FLASK_DEBUG=1

# Limpar logs anteriores para começar fresco
echo "Limpando logs anteriores..."
rm -f envio_hsm_matrix/logs/gunicorn_errors.log

echo "Iniciando Gunicorn no modo de diagnóstico..."
gunicorn --bind 0.0.0.0:5000 \
  --workers 1 \
  --threads 1 \
  --timeout 120 \
  --log-level debug \
  --error-logfile envio_hsm_matrix/logs/gunicorn_errors.log \
  --capture-output \
  --access-logfile envio_hsm_matrix/logs/gunicorn_access.log \
  --reload \
  "envio_hsm_matrix.app:create_app()"

# O gunicorn roda em primeiro plano. O script termina quando o Gunicorn for encerrado. 