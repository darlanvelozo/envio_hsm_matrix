import requests
import json
import csv
from datetime import datetime
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

token_primordial = []
MAX_RETRIES = 3  # Número máximo de tentativas para cada requisição
RETRY_DELAY = 2  # Tempo base de espera entre tentativas (segundos)
BATCH_SIZE = 10  # Número de clientes processados em cada lote

# Função para obter um novo token
def new_token():
    """
    Função para obter um novo token de autenticação da API com retry.
    """
    for attempt in range(MAX_RETRIES):
        try:
            url = "https://api.megalinktelecom.hubsoft.com.br/oauth/token"
            data = {
                "client_id": "75",
                "client_secret": "JCqEuHLcam8zt0mYGvJVP8rZpNJFA2hf7aMrhGmM",
                "username": "api.hub.buzzlead@megalinkinternet.com.br",
                "password": "Api#5554",
                "grant_type": "password"
            }

            response = requests.post(url, json=data, timeout=30)
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

def buscar_cliente_financeiro(codigo_cliente, base_url="https://api.megalinktelecom.hubsoft.com.br"):
    """
    Realiza uma requisição para buscar informações financeiras de um cliente pelo código.
    Implementa retry em caso de falhas.
    
    Args:
        codigo_cliente (str): Código do cliente para busca
        base_url (str, opcional): URL base da API. Padrão: "https://api.megalinktelecom.hubsoft.com.br"
        
    Returns:
        dict: Dados da resposta da API em formato JSON
    """
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

def extrair_faturas_vencidas(dados_cliente):
    """
    Extrai faturas vencidas dos dados do cliente.
    
    Args:
        dados_cliente (dict): Dados do cliente retornados pela API
        
    Returns:
        list: Lista de faturas vencidas ordenadas da mais antiga para a mais recente
    """
    faturas_vencidas = []
    
    if 'faturas' in dados_cliente and dados_cliente['faturas']:
        for fatura in dados_cliente['faturas']:
            if fatura.get('status') == 'vencido' and not fatura.get('quitado'):
                faturas_vencidas.append(fatura)
    
    # Ordenar as faturas por data de vencimento (da mais antiga para a mais recente)
    if faturas_vencidas:
        faturas_vencidas.sort(key=lambda x: datetime.strptime(x.get('data_vencimento', '01/01/2099'), '%d/%m/%Y'))
    
    return faturas_vencidas

def obter_fatura_mais_antiga(dados_cliente):
    """
    Obtém a fatura vencida mais antiga do cliente.
    
    Args:
        dados_cliente (dict): Dados do cliente retornados pela API
        
    Returns:
        dict: A fatura vencida mais antiga ou None se não houver faturas vencidas
    """
    faturas_vencidas = extrair_faturas_vencidas(dados_cliente)
    
    if faturas_vencidas:
        return faturas_vencidas[0]  # A primeira fatura é a mais antiga, pois já ordenamos na função anterior
    else:
        return None

def salvar_fatura_vencida_csv(fatura, codigo_cliente, telefone=""):
    """
    Salva uma única fatura vencida em um arquivo CSV no formato numerado solicitado.
    
    Args:
        fatura (dict): Fatura vencida
        codigo_cliente (str): Código do cliente
        telefone (str, opcional): Número de telefone do cliente
    """
    if not fatura:
        print("Não foi encontrada fatura vencida para este cliente.")
        return
    
    data_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"fatura_vencida_{codigo_cliente}_{data_atual}.csv"
    
    nome_cliente = fatura.get('cliente', {}).get('nome_razaosocial', '')
    
    with open(nome_arquivo, 'w', newline='', encoding='utf-8') as arquivo:
        # Colunas específicas solicitadas com números
        campos = ['Telefone', 'Nome', '1', '2', '3', '4', '5', '6']
        writer = csv.DictWriter(arquivo, fieldnames=campos)
        
        writer.writeheader()
        writer.writerow({
            'Telefone': telefone,
            'Nome': nome_cliente,
            '1': nome_cliente,                     # Nome do cliente
            '2': fatura.get('valor', ''),          # Valor da fatura
            '3': fatura.get('data_vencimento', ''),# Data de vencimento
            '4': fatura.get('linha_digitavel', ''),# Código de barras
            '5': fatura.get('pix_copia_cola', ''), # PIX copia e cola
            '6': fatura.get('link', '')            # Link do boleto
        })
    
    print(f"Arquivo CSV da fatura vencida mais antiga salvo com sucesso: {nome_arquivo}")
    return nome_arquivo

def processar_cliente(codigo_cliente, telefone=""):
    """
    Processa um único cliente, buscando suas faturas vencidas.
    
    Args:
        codigo_cliente (str): Código do cliente
        telefone (str, opcional): Número de telefone do cliente
        
    Returns:
        dict: Dados da fatura vencida ou None
    """
    try:
        dados_cliente = buscar_cliente_financeiro(codigo_cliente)
        fatura = obter_fatura_mais_antiga(dados_cliente)
        
        if fatura:
            nome_cliente = fatura.get('cliente', {}).get('nome_razaosocial', '')
            return {
                'codigo_cliente': codigo_cliente,
                'Telefone': telefone,
                'Nome': nome_cliente,
                '1': nome_cliente,                     # Nome do cliente
                '2': fatura.get('valor', ''),          # Valor da fatura
                '3': fatura.get('data_vencimento', ''),# Data de vencimento
                '4': fatura.get('linha_digitavel', ''),# Código de barras
                '5': fatura.get('pix_copia_cola', ''), # PIX copia e cola
                '6': fatura.get('link', '')            # Link do boleto
            }
        return None
    except Exception as e:
        print(f"Erro ao processar cliente {codigo_cliente}: {str(e)}")
        return None

def processar_csv_lotes(arquivo_csv):
    """
    Processa um arquivo CSV contendo códigos de clientes em lotes para evitar sobrecarga.
    
    Args:
        arquivo_csv (str): Caminho para o arquivo CSV
        
    Returns:
        list: Lista de resultados processados
    """
    resultados = []
    
    try:
        # Ler o arquivo CSV
        with open(arquivo_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            clientes = list(reader)
        
        total_clientes = len(clientes)
        print(f"Processando {total_clientes} clientes em lotes de {BATCH_SIZE}...")
        
        # Processar em lotes
        for i in range(0, total_clientes, BATCH_SIZE):
            batch = clientes[i:i+BATCH_SIZE]
            print(f"Processando lote {i//BATCH_SIZE + 1}/{(total_clientes+BATCH_SIZE-1)//BATCH_SIZE}: clientes {i+1} a {min(i+BATCH_SIZE, total_clientes)}")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_cliente = {}
                
                for cliente in batch:
                    codigo_cliente = cliente.get('codigo_cliente', '')
                    telefone = cliente.get('TelefoneCorrigido', '')
                    if codigo_cliente:
                        future = executor.submit(processar_cliente, codigo_cliente, telefone)
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
        print(f"Erro ao processar arquivo CSV: {str(e)}")
    
    return resultados

def salvar_resultados_csv(resultados, nome_arquivo):
    """
    Salva os resultados em um arquivo CSV.
    
    Args:
        resultados (list): Lista de dicionários com os dados das faturas
        nome_arquivo (str): Nome do arquivo para salvar
    """
    if not resultados:
        print("Não há resultados para salvar.")
        return None
    
    try:
        with open(nome_arquivo, 'w', newline='', encoding='utf-8') as arquivo:
            # Definir campos
            campos = ['Telefone', 'Nome', '1', '2', '3', '4', '5', '6']
            writer = csv.DictWriter(arquivo, fieldnames=campos)
            
            writer.writeheader()
            for resultado in resultados:
                # Remover 'codigo_cliente' antes de escrever no CSV
                if 'codigo_cliente' in resultado:
                    resultado_csv = {k: v for k, v in resultado.items() if k != 'codigo_cliente'}
                else:
                    resultado_csv = resultado
                writer.writerow(resultado_csv)
        
        print(f"Arquivo CSV com {len(resultados)} clientes salvo com sucesso: {nome_arquivo}")
        return nome_arquivo
    except Exception as e:
        print(f"Erro ao salvar resultados: {str(e)}")
        return None

def salvar_resultado_json(dados_cliente, codigo_cliente):
    """
    Salva o resultado completo da consulta financeira em um arquivo JSON.
    
    Args:
        dados_cliente (dict): Dados do cliente retornados pela API
        codigo_cliente (str): Código do cliente
        
    Returns:
        str: Caminho do arquivo salvo
    """
    # Cria um diretório para armazenar os arquivos JSON, se não existir
    diretorio = 'resultados_json'
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)
    
    data_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"{diretorio}/cliente_{codigo_cliente}_{data_atual}.json"
    
    with open(nome_arquivo, 'w', encoding='utf-8') as arquivo:
        json.dump(dados_cliente, arquivo, ensure_ascii=False, indent=4)
    
    print(f"Arquivo JSON salvo com sucesso: {nome_arquivo}")
    return nome_arquivo

# Exemplo de uso
if __name__ == "__main__":
    print("Escolha uma opção:")
    print("1 - Processar um único cliente")
    print("2 - Processar arquivo CSV com múltiplos clientes")
    
    opcao = input("Digite a opção desejada (1 ou 2): ")
    
    if opcao == "1":
        # Solicita ao usuário o código do cliente
        codigo_exemplo = input("Digite o código do cliente: ")
        if not codigo_exemplo:
            codigo_exemplo = "24771"  # Valor padrão
            print(f"Usando código padrão: {codigo_exemplo}")
        
        # Solicita telefone (opcional)
        telefone = input("Digite o telefone do cliente (opcional): ")
        
        # Busca informações do cliente
        resultado = buscar_cliente_financeiro(codigo_exemplo)
        
        # Salvar o resultado completo em JSON
        arquivo_json = salvar_resultado_json(resultado, codigo_exemplo)
        
        # Obter a fatura vencida mais antiga
        fatura_mais_antiga = obter_fatura_mais_antiga(resultado)
        
        # Salvar apenas a fatura vencida mais antiga em CSV com o formato solicitado
        if fatura_mais_antiga:
            arquivo_csv = salvar_fatura_vencida_csv(fatura_mais_antiga, codigo_exemplo, telefone)
            print(f"Arquivo CSV gerado: {arquivo_csv}")
        else:
            print("Não foram encontradas faturas vencidas para o cliente.")
    
    elif opcao == "2":
        arquivo_csv = input("Digite o caminho do arquivo CSV: ")
        
        if not os.path.exists(arquivo_csv):
            print(f"Arquivo {arquivo_csv} não encontrado.")
        else:
            print(f"Processando arquivo {arquivo_csv}...")
            
            # Processar o arquivo CSV em lotes
            resultados = processar_csv_lotes(arquivo_csv)
            
            # Salvar resultados em um único arquivo CSV
            if resultados:
                data_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_arquivo_resultado = f"faturas_vencidas_{data_atual}.csv"
                arquivo_resultado = salvar_resultados_csv(resultados, nome_arquivo_resultado)
                
                if arquivo_resultado:
                    print(f"Processamento concluído! {len(resultados)} clientes com faturas vencidas encontrados.")
                    print(f"Resultados salvos em: {arquivo_resultado}")
                else:
                    print("Erro ao salvar o arquivo de resultados.")
            else:
                print("Nenhum cliente com faturas vencidas encontrado.")
    
    else:
        print("Opção inválida.")
