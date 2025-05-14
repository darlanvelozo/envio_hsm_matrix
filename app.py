from flask import Flask, render_template, request, send_file, make_response, jsonify, url_for
import requests
import json
import csv
import os
import io
from datetime import datetime
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

token_primordial = []
MAX_RETRIES = 3  # Número máximo de tentativas para cada requisição
RETRY_DELAY = 2  # Tempo base de espera entre tentativas (segundos)
BATCH_SIZE = 10  # Número de clientes processados em cada lote

# Variáveis globais para acompanhar o progresso
progresso = {'total': 0, 'processados': 0, 'porcentagem': 0, 'clientes_sem_faturas': []}

# Variável para armazenar o último CSV gerado
ultimo_csv = None

# Variável para armazenar a API selecionada
api_selecionada = "megalink"  # Valor padrão

# Definir colunas do CSV na ordem correta
COLUNAS_CSV = ['Telefone', 'Nome', 'Email', 'cpf/cnpj', 'id_Mailing', '1', '2', '3', '4', '5', '6']

# Configuração das APIs
API_CONFIG = {
    "megalink": {
        "url_base": "https://api.megalinktelecom.hubsoft.com.br",
        "url_token": "https://api.megalinktelecom.hubsoft.com.br/oauth/token",
        "auth": {
            "client_id": "75",
            "client_secret": "JCqEuHLcam8zt0mYGvJVP8rZpNJFA2hf7aMrhGmM",
            "username": "api.hub.buzzlead@megalinkinternet.com.br",
            "password": "Api#5554",
            "grant_type": "password"
        }
    },
    "bjfibra": {
        "url_base": "https://api.bjfibra.hubsoft.com.br",
        "url_token": "https://api.bjfibra.hubsoft.com.br/oauth/token",
        "auth": {
            "client_id": "111",
            "client_secret": "tHs3IHJkzdEjcwvKbMdnRFdmhX5bBt7yDiHMbn7o",
            "username": "matrix_api@megalinkinternet.com.br",
            "password": "PDW1*^k1C4CitaLq9QtJ8*hA2HPc2@",
            "grant_type": "password"
        }
    }
}

def reset_progresso():
    """Reseta as variáveis de progresso para um novo processamento"""
    global progresso
    progresso = {'total': 0, 'processados': 0, 'porcentagem': 0, 'clientes_sem_faturas': []}

def new_token():
    """
    Função para obter um novo token de autenticação da API com retry.
    """
    global api_selecionada
    for attempt in range(MAX_RETRIES):
        try:
            # Obter configuração da API selecionada
            config = API_CONFIG[api_selecionada]
            url_token = config["url_token"]
            auth_data = config["auth"]

            response = requests.post(url_token, json=auth_data, timeout=30)
            if response.status_code == 200:
                token_primordial.clear()
                token_primordial.append(response.json()['access_token'])
                return token_primordial[0]
            elif response.status_code == 429:  # Rate limiting
                wait_time = RETRY_DELAY * (attempt + 1) + random.uniform(0, 1)
                print(f"Rate limit atingido. Aguardando {wait_time:.2f} segundos antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                print(f"Erro ao obter token (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code} - {response.text}")
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"Exceção ao obter token (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    raise Exception("Falha ao obter token após várias tentativas")

def buscar_cliente_financeiro(codigo_cliente):
    """
    Realiza uma requisição para buscar informações financeiras de um cliente pelo código.
    Implementa retry em caso de falhas.
    
    Args:
        codigo_cliente (str): Código do cliente para busca
        
    Returns:
        dict: Dados da resposta da API em formato JSON
    """
    global api_selecionada
    base_url = API_CONFIG[api_selecionada]["url_base"]
    endpoint = f"/api/v1/integracao/cliente/financeiro?busca=codigo_cliente&termo_busca={codigo_cliente}"
    url = f"{base_url}{endpoint}"
    
    for attempt in range(MAX_RETRIES):
        try:
            # Obtém um novo token a cada tentativa para garantir que está válido
            token = new_token()
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}'
            }
            
            payload = {}
            
            response = requests.request("GET", url, headers=headers, data=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limiting
                wait_time = RETRY_DELAY * (attempt + 1) + random.uniform(0, 1)
                print(f"Rate limit atingido ao buscar cliente {codigo_cliente}. Aguardando {wait_time:.2f} segundos...")
                time.sleep(wait_time)
            else:
                print(f"Erro ao buscar cliente {codigo_cliente} (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code} - {response.text}")
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"Exceção ao buscar cliente {codigo_cliente} (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    print(f"Falha ao buscar cliente {codigo_cliente} após {MAX_RETRIES} tentativas")
    return {"status": "error", "msg": f"Falha após {MAX_RETRIES} tentativas", "faturas": []}

def obter_fatura_mais_antiga(dados_cliente):
    """
    Obtém a fatura vencida mais antiga do cliente.
    
    Args:
        dados_cliente (dict): Dados do cliente retornados pela API
        
    Returns:
        dict: A fatura vencida mais antiga ou None se não houver faturas vencidas
    """
    if 'faturas' not in dados_cliente or not dados_cliente['faturas']:
        return None
        
    faturas_vencidas = []
    
    for fatura in dados_cliente['faturas']:
        if fatura.get('status') == 'vencido' and not fatura.get('quitado'):
            faturas_vencidas.append(fatura)
    
    # Ordenar as faturas por data de vencimento (da mais antiga para a mais recente)
    if faturas_vencidas:
        faturas_vencidas.sort(key=lambda x: datetime.strptime(x.get('data_vencimento', '01/01/2099'), '%d/%m/%Y'))
        return faturas_vencidas[0]  # A primeira fatura é a mais antiga
    else:
        return None

def processar_cliente(codigo_cliente, telefone="", nome_exibicao=""):
    """
    Processa um único cliente, buscando suas faturas vencidas.
    
    Args:
        codigo_cliente (str): Código do cliente
        telefone (str, opcional): Número de telefone do cliente
        nome_exibicao (str, opcional): Nome do cliente para exibição (se disponível)
        
    Returns:
        dict: Dados da fatura vencida ou None
    """
    global progresso
    try:
        dados_cliente = buscar_cliente_financeiro(codigo_cliente)
        fatura = obter_fatura_mais_antiga(dados_cliente)
        
        # Incrementar o contador de processados
        progresso['processados'] += 1
        progresso['porcentagem'] = int((progresso['processados'] / progresso['total']) * 100)
        
        if fatura:
            nome_cliente = fatura.get('cliente', {}).get('nome_razaosocial', '')
            return {
                'Telefone': telefone,
                'Nome': nome_cliente,
                'Email': '',                         # Coluna vazia para Email
                'cpf/cnpj': '',                      # Coluna vazia para cpf/cnpj
                'id_Mailing': '',                    # Coluna vazia para id_Mailing
                '1': nome_cliente,                   # Nome do cliente
                '2': fatura.get('valor', ''),        # Valor da fatura
                '3': fatura.get('data_vencimento', ''), # Data de vencimento
                '4': fatura.get('linha_digitavel', ''), # Código de barras
                '5': fatura.get('pix_copia_cola', ''), # PIX copia e cola
                '6': fatura.get('link', '')           # Link do boleto
            }
        else:
            # Registrar cliente sem faturas
            nome_display = nome_exibicao if nome_exibicao else f"Cliente {codigo_cliente}"
            progresso['clientes_sem_faturas'].append({
                'codigo': codigo_cliente,
                'nome': nome_display,
                'telefone': telefone
            })
            return None
    except Exception as e:
        print(f"Erro ao processar cliente {codigo_cliente}: {str(e)}")
        progresso['clientes_sem_faturas'].append({
            'codigo': codigo_cliente,
            'nome': nome_exibicao if nome_exibicao else f"Cliente {codigo_cliente}",
            'telefone': telefone,
            'erro': str(e)
        })
        return None

def processar_csv_lotes(df):
    """
    Processa um DataFrame contendo códigos de clientes em lotes para evitar sobrecarga.
    
    Args:
        df (DataFrame): DataFrame pandas com os dados dos clientes
        
    Returns:
        list: Lista de resultados processados
    """
    global progresso
    resultados = []
    
    try:
        # Converter DataFrame para lista de dicionários
        clientes = df.to_dict('records')
        total_clientes = len(clientes)
        
        if total_clientes == 0:
            return []
        
        # Inicializar variáveis de progresso
        progresso['total'] = total_clientes
        progresso['processados'] = 0
        progresso['porcentagem'] = 0
        
        print(f"Processando {total_clientes} clientes em lotes de {BATCH_SIZE}...")
        
        # Processar em lotes
        for i in range(0, total_clientes, BATCH_SIZE):
            batch = clientes[i:i+BATCH_SIZE]
            print(f"Processando lote {i//BATCH_SIZE + 1}/{(total_clientes+BATCH_SIZE-1)//BATCH_SIZE}: clientes {i+1} a {min(i+BATCH_SIZE, total_clientes)}")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_cliente = {}
                
                for cliente in batch:
                    codigo_cliente = str(cliente.get('codigo_cliente', ''))
                    telefone = str(cliente.get('TelefoneCorrigido', ''))
                    nome = str(cliente.get('Nome', ''))
                    if codigo_cliente:
                        future = executor.submit(processar_cliente, codigo_cliente, telefone, nome)
                        future_to_cliente[future] = codigo_cliente
                
                for future in as_completed(future_to_cliente):
                    codigo = future_to_cliente[future]
                    try:
                        resultado = future.result()
                        if resultado:
                            resultados.append(resultado)
                            print(f"Cliente {codigo} processado com sucesso")
                        else:
                            print(f"Cliente {codigo} não possui faturas vencidas")
                    except Exception as e:
                        print(f"Erro ao processar cliente {codigo}: {str(e)}")
            
            # Aguardar um pouco entre lotes para evitar sobrecarga na API
            if i + BATCH_SIZE < total_clientes:
                espera = 2
                print(f"Aguardando {espera} segundos antes do próximo lote...")
                time.sleep(espera)
    
    except Exception as e:
        print(f"Erro ao processar clientes: {str(e)}")
    
    return resultados

def salvar_csv_temp(resultados):
    """
    Salva os resultados em um arquivo CSV temporário.
    
    Args:
        resultados (list): Lista de dados das faturas vencidas
        
    Returns:
        str: Caminho do arquivo CSV gerado
    """
    global ultimo_csv
    
    # Criar diretório temporário se não existir
    temp_dir = 'temp_csv'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    # Gerar nome de arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"{temp_dir}/faturas_vencidas_{timestamp}.csv"
    
    # Escrever dados no arquivo CSV
    with open(nome_arquivo, 'w', newline='', encoding='utf-8') as arquivo:
        writer = csv.DictWriter(arquivo, fieldnames=COLUNAS_CSV)
        writer.writeheader()
        for resultado in resultados:
            writer.writerow(resultado)
    
    # Atualizar a variável global com o caminho do último arquivo gerado
    ultimo_csv = nome_arquivo
    
    return nome_arquivo

@app.route('/progresso', methods=['GET'])
def obter_progresso():
    """Endpoint para fornecer informações atualizadas sobre o progresso do processamento"""
    global progresso
    return jsonify(progresso)

@app.route('/download-ultimo-csv', methods=['GET'])
def download_ultimo_csv():
    """Endpoint para baixar o último CSV gerado"""
    global ultimo_csv
    
    if not ultimo_csv or not os.path.exists(ultimo_csv):
        return render_template('index.html', error='Nenhum arquivo CSV disponível para download')
    
    return send_file(ultimo_csv, as_attachment=True, download_name='faturas_vencidas.csv')

@app.route('/', methods=['GET', 'POST'])
def index():
    global api_selecionada
    
    if request.method == 'POST':
        # Verificar se foi enviado um arquivo ou apenas trocou a API
        if 'hubsoft_api' in request.form:
            api_selecionada = request.form['hubsoft_api']
            # Se for apenas a troca da API, retornar a página inicial
            if 'file' not in request.files:
                return render_template('index.html', api_selecionada=api_selecionada)
        
        # Definir a API selecionada
        if 'hubsoft_api' in request.form:
            api_selecionada = request.form['hubsoft_api']
        
        # Processar o arquivo enviado
        if 'file' not in request.files:
            return render_template('index.html', error='Nenhum arquivo selecionado', api_selecionada=api_selecionada)
        
        file = request.files['file']
        
        if file.filename == '':
            return render_template('index.html', error='Nenhum arquivo selecionado', api_selecionada=api_selecionada)
        
        if file and file.filename.endswith('.csv'):
            try:
                # Resetar variáveis de progresso
                reset_progresso()
                
                # Ler o CSV
                df = pd.read_csv(file, encoding='utf-8')
                
                # Verificar se existe a coluna código_cliente
                if 'codigo_cliente' not in df.columns:
                    return render_template('index.html', 
                                          error="O arquivo CSV deve conter uma coluna 'codigo_cliente'", 
                                          api_selecionada=api_selecionada)
                
                # Processar o arquivo CSV em lotes
                resultados = processar_csv_lotes(df)
                
                if not resultados:
                    return render_template('index.html', 
                                          error='Nenhuma fatura vencida encontrada para os clientes fornecidos',
                                          clientes_sem_faturas=progresso['clientes_sem_faturas'],
                                          api_selecionada=api_selecionada)
                
                # Salvar os resultados em um arquivo CSV temporário
                arquivo_csv = salvar_csv_temp(resultados)
                
                # Se for uma requisição AJAX, retornar apenas o CSV
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return send_file(arquivo_csv, as_attachment=True, download_name='faturas_vencidas.csv')
                
                # Para requisições normais, renderizar o template com informações de sucesso
                return render_template('index.html', 
                                      sucesso=True, 
                                      clientes_sem_faturas=progresso['clientes_sem_faturas'],
                                      num_processados=progresso['processados'],
                                      num_encontrados=len(resultados),
                                      download_url=url_for('download_ultimo_csv'),
                                      api_selecionada=api_selecionada)
                
            except Exception as e:
                error_msg = str(e)
                return render_template('index.html', 
                                      error=f'Erro ao processar o arquivo: {error_msg}',
                                      api_selecionada=api_selecionada)
        else:
            return render_template('index.html', 
                                  error='Arquivo inválido. Por favor, selecione um arquivo CSV.',
                                  api_selecionada=api_selecionada)
    
    # Para requisição GET, apenas renderizar o template inicial
    return render_template('index.html', api_selecionada=api_selecionada)

if __name__ == '__main__':
    # Certifique-se de que a pasta templates existe
    if not os.path.exists('templates'):
        os.mkdir('templates')
    
    app.run(debug=True)