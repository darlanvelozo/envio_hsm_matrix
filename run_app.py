import os
import webbrowser
import threading
import time
from app import app

def open_browser():
    """Função para abrir o navegador após alguns segundos"""
    # Dar tempo para o servidor iniciar
    time.sleep(1.5)
    # Abrir o navegador na URL da aplicação
    webbrowser.open('http://127.0.0.1:5000/')

if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    if not os.path.exists('templates'):
        os.mkdir('templates')
    if not os.path.exists('temp_csv'):
        os.mkdir('temp_csv')
    
    # Iniciar thread para abrir o navegador
    threading.Thread(target=open_browser).start()
    
    # Iniciar o servidor Flask
    print("Iniciando a aplicação Processador de Faturas Vencidas...")
    print("Aguarde, o navegador será aberto automaticamente...")
    
    # Usar host 0.0.0.0 permitirá acesso de outras máquinas na rede
    # Se quiser acesso apenas local, use 127.0.0.1
    app.run(host='127.0.0.1', port=5000, debug=False) 