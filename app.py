from flask import Flask, render_template, request, send_file, make_response, jsonify, url_for
import requests
import json
import csv
import os
import io
import stat
from datetime import datetime
import pandas as pd
import time
import random
import sys
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# Diretório base da aplicação
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Criar diretório de logs com permissões adequadas
logs_dir = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(logs_dir):
    try:
        os.makedirs(logs_dir, exist_ok=True)
        os.chmod(logs_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
    except Exception as e:
        print(f"ERRO: Não foi possível criar o diretório de logs: {str(e)}")
        sys.exit(1)

# Verificar permissões de escrita no diretório de logs
if not os.access(logs_dir, os.W_OK):
    print(f"ERRO: Não há permissão de escrita no diretório de logs: {logs_dir}")
    print("Por favor, ajuste as permissões com: chmod -R 755 envio_hsm_matrix/logs")
    sys.exit(1)

# Configurar logging
log_file = os.path.join(logs_dir, 'app.log')
try:
    # Testar se é possível criar/escrever no arquivo de log
    with open(log_file, 'a') as f:
        pass  # Só para verificar se conseguimos abrir para escrita
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, 'a')
        ]
    )
except Exception as e:
    print(f"ERRO: Não foi possível configurar o logging: {str(e)}")
    print(f"Verifique as permissões do arquivo {log_file}")
    sys.exit(1)

logger = logging.getLogger(__name__)

# Aumentar timeout padrão para requisições HTTP
TIMEOUT_PADRAO = 60  # Timeout de 60 segundos para requisições HTTP

# Configurações de processamento
token_primordial = []
MAX_RETRIES = 3           # Número máximo de tentativas para cada requisição
RETRY_DELAY = 2           # Tempo base de espera entre tentativas (segundos)
BATCH_SIZE = 10           # Número de clientes processados em cada lote
MAX_WORKERS = 5           # Número máximo de threads para processamento paralelo
FUTURE_TIMEOUT = 60       # Timeout para cada future (em segundos)

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

# Criar a aplicação Flask
app = Flask(__name__)

def get_absolute_path(path):
    """
    Converte um caminho relativo para absoluto.
    """
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_DIR, path)

def reset_progresso():
    """Reseta as variáveis de progresso para um novo processamento"""
    global progresso
    progresso = {'total': 0, 'processados': 0, 'porcentagem': 0, 'clientes_sem_faturas': []}

def salvar_ultimo_csv(caminho):
    """
    Salva o caminho do último CSV gerado em um arquivo para persistência entre sessões.
    """
    try:
        cache_dir = get_absolute_path('cache')
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, 'ultimo_csv.txt')
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(caminho)
        
        logger.info(f"Caminho do último CSV salvo em cache: {caminho}")
    except Exception as e:
        logger.error(f"Erro ao salvar caminho do último CSV: {str(e)}")

def carregar_ultimo_csv():
    """
    Carrega o caminho do último CSV gerado de um arquivo de cache.
    """
    try:
        cache_file = get_absolute_path('cache/ultimo_csv.txt')
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                caminho = f.read().strip()
                
            # Verificar se o arquivo realmente existe
            if os.path.exists(caminho):
                logger.info(f"Último CSV carregado do cache: {caminho}")
                return caminho
    except Exception as e:
        logger.error(f"Erro ao carregar caminho do último CSV: {str(e)}")
    
    return None

def verificar_e_preparar_diretorios():
    """
    Verifica e cria os diretórios necessários com as permissões adequadas.
    """
    diretorios = ['temp_csv', 'logs', 'templates', 'cache']
    
    for diretorio in diretorios:
        dir_path = get_absolute_path(diretorio)
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path, exist_ok=True)
                # Garantir permissões de leitura/escrita
                os.chmod(dir_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
                logger.info(f"Diretório '{dir_path}' criado com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao criar diretório '{dir_path}': {str(e)}")
                raise Exception(f"Não foi possível criar o diretório '{dir_path}': {str(e)}")
        
        # Verificar se o diretório tem permissões de escrita
        if not os.access(dir_path, os.W_OK):
            logger.error(f"O diretório '{dir_path}' não tem permissões de escrita!")
            raise Exception(f"O diretório '{dir_path}' não tem permissões de escrita. Por favor, corrija as permissões.")

def inicializar_app():
    """
    Inicializa a aplicação, preparando diretórios e carregando o último CSV.
    Esta função deve ser chamada na inicialização da aplicação, seja pelo Flask ou pelo Gunicorn.
    """
    global ultimo_csv
    try:
        # Garantir que os diretórios necessários existam
        verificar_e_preparar_diretorios()
        
        # Carregar último CSV, se existir
        if ultimo_csv is None:
            ultimo_csv = carregar_ultimo_csv()
        
        logger.info("Aplicação inicializada com sucesso")
    except Exception as e:
        logger.error(f"Erro ao inicializar aplicação: {str(e)}")
        # Não vamos encerrar o aplicativo aqui, apenas registrar o erro

# Executar a inicialização
inicializar_app()

def new_token():
    """
    Obtém um novo token de autenticação da API com retry.
    """
    global api_selecionada
    for attempt in range(MAX_RETRIES):
        try:
            # Obter configuração da API selecionada
            config = API_CONFIG[api_selecionada]
            url_token = config["url_token"]
            auth_data = config["auth"]

            response = requests.post(url_token, json=auth_data, timeout=TIMEOUT_PADRAO)
            if response.status_code == 200:
                token_primordial.clear()
                token_primordial.append(response.json()['access_token'])
                return token_primordial[0]
            elif response.status_code == 429:  # Rate limiting
                wait_time = RETRY_DELAY * (attempt + 1) + random.uniform(0, 1)
                logger.warning(f"Rate limit atingido. Aguardando {wait_time:.2f} segundos antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                logger.error(f"Erro ao obter token (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code} - {response.text}")
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            logger.error(f"Exceção ao obter token (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    raise Exception("Falha ao obter token após várias tentativas")

def buscar_cliente_financeiro(codigo_cliente):
    """
    Realiza uma requisição para buscar informações financeiras de um cliente.
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
            
            response = requests.request("GET", url, headers=headers, data=payload, timeout=TIMEOUT_PADRAO)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limiting
                wait_time = RETRY_DELAY * (attempt + 1) + random.uniform(0, 1)
                logger.warning(f"Rate limit atingido ao buscar cliente {codigo_cliente}. Aguardando {wait_time:.2f} segundos...")
                time.sleep(wait_time)
            else:
                logger.error(f"Erro ao buscar cliente {codigo_cliente} (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code} - {response.text}")
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout ao buscar cliente {codigo_cliente} (tentativa {attempt+1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            logger.error(f"Exceção ao buscar cliente {codigo_cliente} (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    logger.error(f"Falha ao buscar cliente {codigo_cliente} após {MAX_RETRIES} tentativas")
    return {"status": "error", "msg": f"Falha após {MAX_RETRIES} tentativas", "faturas": []}

def obter_fatura_mais_antiga(dados_cliente):
    """
    Obtém a fatura vencida mais antiga do cliente.
    """
    if 'faturas' not in dados_cliente or not dados_cliente['faturas']:
        return None
        
    faturas_vencidas = []
    
    for fatura in dados_cliente['faturas']:
        if fatura.get('status') == 'vencido' and not fatura.get('quitado'):
            faturas_vencidas.append(fatura)
    
    # Ordenar as faturas por data de vencimento (da mais antiga para a mais recente)
    if faturas_vencidas:
        try:
            faturas_vencidas.sort(key=lambda x: datetime.strptime(x.get('data_vencimento', '01/01/2099'), '%d/%m/%Y'))
            
            # Analisar a estrutura da primeira fatura para diagnóstico
            fatura_mais_antiga = faturas_vencidas[0]
            
            # Registro de debug especial para o valor
            logger.info(f"VALOR DA FATURA: {fatura_mais_antiga.get('valor')}")
            logger.info(f"TIPO DO VALOR: {type(fatura_mais_antiga.get('valor'))}")
            
            # Registrar conteúdo da fatura para diagnóstico
            logger.info(f"Conteúdo da fatura mais antiga (ID: {fatura_mais_antiga.get('id_fatura')})")
            logger.info(f"Campos disponíveis: {', '.join(fatura_mais_antiga.keys())}")
            
            # Registrar todos os campos relacionados a valor
            for chave, valor in fatura_mais_antiga.items():
                if 'valor' in chave.lower():
                    logger.info(f"Campo de valor encontrado: '{chave}' = '{valor}' (tipo: {type(valor)})")
            
            return fatura_mais_antiga  # A primeira fatura é a mais antiga
        except Exception as e:
            logger.error(f"Erro ao ordenar faturas: {str(e)}")
            # Em caso de erro, retornar a primeira fatura encontrada
            return faturas_vencidas[0]
    else:
        return None

def limpar_texto_para_csv(texto):
    """
    Limpa o texto para evitar problemas com caracteres especiais no CSV
    e remove espaços em branco no início e fim do texto.
    """
    if not texto or not isinstance(texto, str):
        return ""
    
    # Remover caracteres que podem causar problemas no CSV
    texto = texto.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    
    # Remover caracteres nulos ou de controle
    texto = ''.join(char for char in texto if ord(char) >= 32 or char in '\t\n\r')
    
    # Remover espaços em branco no início e fim do texto
    texto = texto.strip()
    
    # Remover espaços duplos ou múltiplos dentro do texto
    while '  ' in texto:
        texto = texto.replace('  ', ' ')
    
    return texto

def processar_cliente(codigo_cliente, telefone="", nome_exibicao=""):
    """
    Processa um único cliente, buscando suas faturas vencidas.
    """
    global progresso
    try:
        dados_cliente = buscar_cliente_financeiro(codigo_cliente)
        fatura = obter_fatura_mais_antiga(dados_cliente)
        
        # Incrementar o contador de processados
        progresso['processados'] += 1
        progresso['porcentagem'] = int((progresso['processados'] / progresso['total']) * 100)
        
        if fatura:
            # Registrar fatura para diagnóstico
            logger.info(f"Processando fatura para cliente {codigo_cliente}: {fatura.get('id_fatura', 'sem ID')}")
            
            # Obter dados básicos
            nome_cliente = fatura.get('cliente', {}).get('nome_razaosocial', '')
            
            # Obter o valor diretamente do campo 'valor' da fatura
            valor_bruto = fatura.get('valor')
            
            # Log para diagnóstico
            logger.info(f"Valor da fatura (bruto): {valor_bruto} (tipo: {type(valor_bruto)})")
            
            # Formatar valor monetário
            if valor_bruto is not None:
                if isinstance(valor_bruto, (int, float)):
                    valor = f"R$ {valor_bruto:.2f}".replace('.', ',')
                else:
                    valor = str(valor_bruto)
            else:
                valor = ""
            
            # Limpar textos para evitar problemas no CSV
            nome_cliente_limpo = limpar_texto_para_csv(nome_cliente)
            telefone_limpo = limpar_texto_para_csv(telefone)
            valor_limpo = limpar_texto_para_csv(valor)
            data_vencimento_limpo = limpar_texto_para_csv(fatura.get('data_vencimento', ''))
            linha_digitavel_limpo = limpar_texto_para_csv(fatura.get('linha_digitavel', ''))
            pix_limpo = limpar_texto_para_csv(fatura.get('pix_copia_cola', ''))
            link_limpo = limpar_texto_para_csv(fatura.get('link', ''))
            
            # Log para diagnóstico final
            logger.info(f"Valor formatado para cliente {codigo_cliente}: '{valor}' -> limpo: '{valor_limpo}'")
            
            return {
                'Telefone': telefone_limpo,
                'Nome': nome_cliente_limpo,
                'Email': '',
                'cpf/cnpj': '',
                'id_Mailing': '',
                '1': nome_cliente_limpo,
                '2': valor_limpo,
                '3': data_vencimento_limpo,
                '4': linha_digitavel_limpo,
                '5': pix_limpo,
                '6': link_limpo
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
        logger.error(f"Erro ao processar cliente {codigo_cliente}: {str(e)}")
        progresso['clientes_sem_faturas'].append({
            'codigo': codigo_cliente,
            'nome': nome_exibicao if nome_exibicao else f"Cliente {codigo_cliente}",
            'telefone': telefone,
            'erro': str(e)
        })
        return None

def processar_csv_lotes(df):
    """
    Processa um DataFrame contendo códigos de clientes em lotes.
    Usa ThreadPoolExecutor para processamento paralelo com timeouts e retry.
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
        
        logger.info(f"Processando {total_clientes} clientes em lotes de {BATCH_SIZE}...")
        
        # Processar em lotes
        for i in range(0, total_clientes, BATCH_SIZE):
            batch = clientes[i:i+BATCH_SIZE]
            logger.info(f"Processando lote {i//BATCH_SIZE + 1}/{(total_clientes+BATCH_SIZE-1)//BATCH_SIZE}: clientes {i+1} a {min(i+BATCH_SIZE, total_clientes)}")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
                        # Adicionar timeout para cada future
                        resultado = future.result(timeout=FUTURE_TIMEOUT)
                        if resultado:
                            resultados.append(resultado)
                            logger.debug(f"Cliente {codigo} processado com sucesso")
                        else:
                            logger.debug(f"Cliente {codigo} não possui faturas vencidas")
                    except TimeoutError:
                        logger.warning(f"Timeout ao processar cliente {codigo}")
                        progresso['clientes_sem_faturas'].append({
                            'codigo': codigo,
                            'nome': f"Cliente {codigo}",
                            'telefone': "",
                            'erro': "Timeout ao processar cliente"
                        })
                    except Exception as e:
                        logger.error(f"Erro ao processar cliente {codigo}: {str(e)}")
                        progresso['clientes_sem_faturas'].append({
                            'codigo': codigo,
                            'nome': f"Cliente {codigo}",
                            'telefone': "",
                            'erro': str(e)
                        })
            
            # Aguardar um pouco entre lotes para evitar sobrecarga na API
            if i + BATCH_SIZE < total_clientes:
                espera = 2
                logger.info(f"Aguardando {espera} segundos antes do próximo lote...")
                time.sleep(espera)
    
    except Exception as e:
        logger.error(f"Erro ao processar clientes: {str(e)}")
    
    return resultados

def limpar_csv_antigos(diretorio='temp_csv', dias=1):
    """
    Remove arquivos CSV antigos do diretório temporário.
    """
    try:
        diretorio_abs = get_absolute_path(diretorio)
        agora = time.time()
        count = 0
        for nome_arquivo in os.listdir(diretorio_abs):
            caminho = os.path.join(diretorio_abs, nome_arquivo)
            if os.path.isfile(caminho):
                tempo_mod = os.path.getmtime(caminho)
                if agora - tempo_mod > dias * 86400:
                    try:
                        os.remove(caminho)
                        count += 1
                    except Exception as e:
                        logger.error(f"Erro ao remover arquivo antigo {caminho}: {e}")
        logger.info(f"Removidos {count} arquivos antigos de {diretorio_abs}")
    except Exception as e:
        logger.error(f"Erro ao limpar arquivos antigos em {diretorio}: {e}")

def verificar_integridade_csv(nome_arquivo):
    """
    Verifica se um arquivo CSV está íntegro.
    """
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            # Ler algumas linhas para verificar se o arquivo está íntegro
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 10:  # Ler até 10 linhas
                    break
                
                # Verificar se há pelo menos uma coluna na linha
                if not row or len(row) == 0:
                    logger.error(f"Linha {i} do CSV está vazia ou mal formatada")
                    return False
        
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar integridade do CSV '{nome_arquivo}': {e}")
        return False

def salvar_csv_temp(resultados):
    """
    Salva os resultados em um arquivo CSV temporário de forma otimizada.
    """
    global ultimo_csv
    
    if not resultados or len(resultados) == 0:
        raise Exception("Nenhum resultado para salvar no CSV")
    
    temp_dir = get_absolute_path('temp_csv')
    try:
        verificar_e_preparar_diretorios()
    except Exception as e:
        raise Exception(f"Erro ao preparar diretórios: {str(e)}")
    
    # Limpar arquivos antigos
    try:
        limpar_csv_antigos(temp_dir, dias=1)
    except Exception as e:
        logger.error(f"Erro ao limpar arquivos antigos: {e}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = os.path.join(temp_dir, f"faturas_vencidas_{timestamp}.csv")
    arquivo_temp = f"{nome_arquivo}.tmp"
    
    try:
        # Primeiro criar um arquivo temporário e depois renomear
        with open(arquivo_temp, 'w', newline='', encoding='utf-8') as arquivo:
            writer = csv.DictWriter(
                arquivo, 
                fieldnames=COLUNAS_CSV,
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                delimiter=',',
                escapechar=None,
                doublequote=True
            )
            writer.writeheader()
            
            # Processar em lotes para economizar memória com arquivos grandes
            tamanho_lote = 1000
            total_lotes = (len(resultados) + tamanho_lote - 1) // tamanho_lote
            
            logger.info(f"Salvando CSV em {total_lotes} lotes")
            
            for i in range(0, len(resultados), tamanho_lote):
                lote = resultados[i:i+tamanho_lote]
                for resultado in lote:
                    # Garantir que nenhum valor tenha espaços extras no início ou fim
                    linha_limpa = {}
                    for coluna, valor in resultado.items():
                        if isinstance(valor, str):
                            linha_limpa[coluna] = valor.strip()
                        else:
                            linha_limpa[coluna] = valor
                    
                    writer.writerow(linha_limpa)
                
                # Flush para cada lote para evitar perda de dados
                arquivo.flush()
                os.fsync(arquivo.fileno())
        
        # Renomear o arquivo temporário para o nome final
        os.rename(arquivo_temp, nome_arquivo)
        
        # Verificar a integridade do arquivo CSV gerado
        if not verificar_integridade_csv(nome_arquivo):
            raise Exception("O arquivo CSV foi gerado, mas falhou na verificação de integridade")
        
        # Confirmar existência e tamanho do arquivo
        if not os.path.exists(nome_arquivo):
            raise Exception(f"Arquivo CSV não foi criado em: {nome_arquivo}")
        
        tamanho = os.path.getsize(nome_arquivo)
        if tamanho == 0:
            raise Exception("Arquivo CSV gerado está vazio")
        
        logger.info(f"CSV gerado com sucesso: {nome_arquivo} ({tamanho} bytes)")
        
        # Salvar o caminho em arquivo para persistência
        salvar_ultimo_csv(nome_arquivo)
        
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo CSV: {str(e)}")
        if os.path.exists(arquivo_temp):
            try:
                os.remove(arquivo_temp)
            except:
                pass
        raise Exception(f"Erro ao salvar arquivo CSV: {str(e)}")
    
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
    
    try:
        # Verificar se há um arquivo na variável global
        if not ultimo_csv:
            # Tentar carregar do arquivo de cache
            ultimo_csv = carregar_ultimo_csv()
            
        if not ultimo_csv or not os.path.exists(ultimo_csv):
            logger.warning(f"Tentativa de download falhou. Caminho do CSV: {ultimo_csv}")
            return render_template('index.html', error='Nenhum arquivo CSV disponível para download. Por favor, tente processar o arquivo novamente.')
        
        # Verificar tamanho e integridade
        tamanho = os.path.getsize(ultimo_csv)
        if tamanho == 0:
            logger.error(f"Arquivo CSV está vazio: {ultimo_csv}")
            return render_template('index.html', error='O arquivo CSV está vazio. Por favor, tente processar o arquivo novamente.')
        
        if not verificar_integridade_csv(ultimo_csv):
            logger.error(f"Arquivo CSV corrompido: {ultimo_csv}")
            return render_template('index.html', error='O arquivo CSV está corrompido. Por favor, tente processá-lo novamente.')
        
        # Criar nome de download com timestamp atual
        download_name = f"faturas_vencidas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            # Método principal de download - usando caminho absoluto
            response = send_file(
                os.path.abspath(ultimo_csv), 
                as_attachment=True, 
                download_name=download_name,
                mimetype='text/csv',
                max_age=0,
                last_modified=datetime.now()
            )
            
            # Adicionar headers anti-cache
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
            
            logger.info(f"Download iniciado: {download_name}")
            return response
            
        except Exception as e:
            logger.error(f"Erro no send_file, tentando método alternativo: {str(e)}")
            
            # Método alternativo de download
            try:
                with open(ultimo_csv, 'rb') as f:
                    csv_data = f.read()
                
                response = make_response(csv_data)
                response.headers["Content-Disposition"] = f"attachment; filename={download_name}"
                response.headers["Content-Type"] = "text/csv"
                response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                response.headers['Pragma'] = 'no-cache'
                response.headers['Expires'] = '0'
                
                logger.info(f"Download alternativo iniciado: {download_name}")
                return response
            except Exception as e2:
                logger.error(f"Falha no método alternativo de download: {str(e2)}")
                # Último recurso: tentar redirecionar para o arquivo
                try:
                    return render_template('index.html', error=f'Erro ao baixar o CSV. Tente novamente.')
                except:
                    return jsonify({'error': 'Falha ao baixar o arquivo.'}), 500
            
    except Exception as e:
        logger.error(f"Erro ao preparar o download do CSV: {str(e)}")
        return render_template('index.html', error=f'Erro ao preparar o download do CSV: {str(e)}')

@app.route('/', methods=['GET', 'POST'])
def index():
    global api_selecionada, ultimo_csv
    
    try:
        logger.info(f"Acessando rota principal. Método: {request.method}")
        
        # Garantir que os diretórios existam
        try:
            verificar_e_preparar_diretorios()
        except Exception as e:
            logger.error(f"Erro ao preparar diretórios: {str(e)}")
            return render_template('index.html', error=f'Erro de configuração: {str(e)}')
        
        if request.method == 'POST':
            logger.info("Processando requisição POST")
            
            if 'hubsoft_api' in request.form:
                api_selecionada = request.form['hubsoft_api']
                logger.info(f"API selecionada: {api_selecionada}")
                if 'file' not in request.files:
                    return render_template('index.html', api_selecionada=api_selecionada)
            
            if 'file' not in request.files:
                logger.warning("Nenhum arquivo foi enviado")
                return render_template('index.html', error='Nenhum arquivo selecionado', api_selecionada=api_selecionada)
            
            file = request.files['file']
            logger.info(f"Arquivo enviado: {file.filename}")
            
            if file.filename == '':
                logger.warning("Nome do arquivo está vazio")
                return render_template('index.html', error='Nenhum arquivo selecionado', api_selecionada=api_selecionada)
            
            if not file.filename.endswith('.csv'):
                logger.warning(f"Tipo de arquivo inválido: {file.filename}")
                return render_template('index.html', 
                                      error='Arquivo inválido. Por favor, selecione um arquivo CSV.',
                                      api_selecionada=api_selecionada)
            
            try:
                # Início do processamento
                logger.info(f"Iniciando processamento do arquivo: {file.filename}")
                reset_progresso()
                
                # Salvar arquivo temporariamente para evitar problemas de memória
                temp_dir = get_absolute_path('temp_csv')
                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir, exist_ok=True)
                    
                temp_file = os.path.join(temp_dir, f"input_{int(time.time())}.csv")
                logger.info(f"Salvando arquivo temporariamente em: {temp_file}")
                file.save(temp_file)
                
                # Verificar se o arquivo foi salvo corretamente
                if not os.path.exists(temp_file):
                    raise Exception(f"Não foi possível salvar o arquivo em {temp_file}")
                
                tamanho = os.path.getsize(temp_file)
                logger.info(f"Arquivo temporário salvo com sucesso. Tamanho: {tamanho} bytes")
                
                # Ler CSV com tratamento de erros de encoding
                try:
                    logger.info("Tentando ler CSV com UTF-8")
                    df = pd.read_csv(temp_file, encoding='utf-8', skipinitialspace=True)
                except UnicodeDecodeError:
                    logger.info("Falha ao ler com UTF-8, tentando Latin-1")
                    try:
                        df = pd.read_csv(temp_file, encoding='latin1', skipinitialspace=True)
                    except Exception as e:
                        logger.error(f"Erro ao ler CSV com Latin-1: {str(e)}")
                        raise Exception(f"Erro ao ler o arquivo CSV. Problema de codificação: {str(e)}")
                finally:
                    # Remover arquivo temporário
                    try:
                        os.remove(temp_file)
                        logger.info(f"Arquivo temporário removido: {temp_file}")
                    except Exception as e:
                        logger.warning(f"Não foi possível remover o arquivo temporário: {str(e)}")
                
                logger.info(f"Colunas no CSV: {', '.join(df.columns)}")
                
                # Verificar colunas necessárias
                if 'codigo_cliente' not in df.columns:
                    logger.error("Coluna 'codigo_cliente' não encontrada no CSV")
                    return render_template('index.html', 
                                          error="O arquivo CSV deve conter uma coluna 'codigo_cliente'", 
                                          api_selecionada=api_selecionada)
                
                # Verificar dados válidos
                if df.empty or df['codigo_cliente'].isna().all():
                    logger.error("CSV vazio ou com todos os códigos de cliente nulos")
                    return render_template('index.html', 
                                          error="O arquivo CSV não contém códigos de cliente válidos", 
                                          api_selecionada=api_selecionada)
                
                # Limpar valores nulos
                df = df.fillna('')
                
                # Converter códigos de cliente para string
                df['codigo_cliente'] = df['codigo_cliente'].astype(str)
                
                # Evitar duplicações
                df = df.drop_duplicates(subset=['codigo_cliente'])
                
                # Informações sobre o dataframe
                logger.info(f"Número de clientes a processar: {len(df)}")
                
                # Processar clientes em lotes
                logger.info("Iniciando processamento de clientes em lotes")
                resultados = processar_csv_lotes(df)
                
                if not resultados:
                    logger.warning("Nenhuma fatura vencida encontrada")
                    return render_template('index.html', 
                                          error='Nenhuma fatura vencida encontrada para os clientes fornecidos',
                                          clientes_sem_faturas=progresso['clientes_sem_faturas'],
                                          api_selecionada=api_selecionada)
                
                logger.info(f"Resultados obtidos: {len(resultados)} faturas")
                
                # Salvar resultados em CSV
                try:
                    logger.info("Salvando resultados em CSV")
                    arquivo_csv = salvar_csv_temp(resultados)
                    logger.info(f"Arquivo CSV salvo com sucesso: {arquivo_csv}")
                    ultimo_csv = arquivo_csv
                except Exception as e:
                    logger.error(f"Erro ao salvar CSV: {str(e)}")
                    return render_template('index.html', 
                                          error=f'Erro ao salvar o arquivo CSV: {str(e)}',
                                          clientes_sem_faturas=progresso['clientes_sem_faturas'],
                                          api_selecionada=api_selecionada)
                
                # Responder de acordo com o tipo de requisição
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    try:
                        logger.info("Enviando CSV via AJAX")
                        return send_file(arquivo_csv, as_attachment=True, download_name=f'faturas_vencidas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                    except Exception as e:
                        logger.error(f"Erro ao enviar arquivo via AJAX: {str(e)}")
                        return jsonify({'error': f'Erro ao preparar download do CSV: {str(e)}'}), 500
                
                # Retorno normal para requisições não-AJAX
                logger.info("Processamento concluído com sucesso")
                return render_template('index.html', 
                                      sucesso=True, 
                                      clientes_sem_faturas=progresso['clientes_sem_faturas'],
                                      num_processados=progresso['processados'],
                                      num_encontrados=len(resultados),
                                      download_url=url_for('download_ultimo_csv'),
                                      api_selecionada=api_selecionada)
            
            except Exception as e:
                logger.error(f"Erro no processamento: {str(e)}", exc_info=True)
                error_msg = str(e)
                return render_template('index.html', 
                                      error=f'Erro ao processar o arquivo: {error_msg}',
                                      api_selecionada=api_selecionada)
        
        # GET request
        logger.info("Processando requisição GET")
        
        # Verificar por CSV existente ao carregar a página
        if not ultimo_csv:
            ultimo_csv = carregar_ultimo_csv()
            if ultimo_csv:
                logger.info(f"CSV encontrado: {ultimo_csv}")
            else:
                logger.info("Nenhum CSV encontrado")
            
        return render_template('index.html', api_selecionada=api_selecionada)
    
    except Exception as e:
        # Capturar qualquer exceção não tratada
        logger.critical(f"Erro não tratado na rota principal: {str(e)}", exc_info=True)
        return render_template('index.html', 
                             error=f"Erro interno do servidor: {str(e)}"), 500

# Tratamento de erros global
@app.errorhandler(Exception)
def handle_exception(e):
    """Handler global para exceções não tratadas"""
    # Obter o traceback completo
    error_traceback = traceback.format_exc()
    
    # Registrar erro detalhado
    logger.error(f"Erro não tratado: {str(e)}")
    logger.error(f"Tipo de erro: {type(e).__name__}")
    logger.error(f"Traceback completo: {error_traceback}")
    
    # Se for uma exceção específica do Flask
    if hasattr(e, 'code') and hasattr(e, 'description'):
        logger.error(f"Erro HTTP: {e.code} - {e.description}")
    
    # Verificar o request atual se disponível
    try:
        if request:
            logger.error(f"URL que causou o erro: {request.url}")
            logger.error(f"Método HTTP: {request.method}")
            logger.error(f"Agente do usuário: {request.user_agent}")
            if request.form:
                logger.error(f"Dados de formulário: {request.form}")
    except Exception:
        pass  # Ignora erros ao tentar acessar o objeto request
    
    # Em produção, retornar uma mensagem genérica para o usuário
    return render_template('index.html', 
                          error="Ocorreu um erro interno no servidor. Por favor tente novamente."), 500

# Configurar aplicação para trabalhar com Gunicorn
def create_app():
    """
    Factory function para criar a aplicação Flask.
    Esta função será chamada pelo Gunicorn para inicializar a aplicação.
    """
    try:
        # Configurar logging mais detalhado para Gunicorn
        logging.basicConfig(
            level=logging.DEBUG,  # Mudar para DEBUG para mais detalhes
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(os.path.join(BASE_DIR, 'logs/gunicorn_errors.log'), 'a')
            ]
        )
        
        # Log para marcar o início da inicialização
        logger.info("Inicializando aplicação via Gunicorn")
        
        # Registrar ambiente
        logger.info(f"Diretório base: {BASE_DIR}")
        logger.info(f"Diretório de trabalho atual: {os.getcwd()}")
        logger.info(f"Ambiente Python: {sys.version}")
        
        # Inicializar a aplicação
        inicializar_app()
        
        # Configurar Flask para não suprimir erros
        app.config['PROPAGATE_EXCEPTIONS'] = True
        app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = True
        
        # Log de conclusão
        logger.info("Aplicação inicializada com sucesso via Gunicorn")
        
        return app
    except Exception as e:
        # Registrar exceção detalhada
        logger.critical(f"ERRO CRÍTICO ao inicializar via Gunicorn: {str(e)}", exc_info=True)
        logger.critical(f"Traceback completo: {traceback.format_exc()}")
        # Não usar sys.exit() aqui, pois isso encerraria o processo do Gunicorn
        # Em vez disso, permite que o erro seja propagado
        raise

# Ponto de entrada para execução direta (não via Gunicorn)
if __name__ == '__main__':
    try:
        # Garantir que a aplicação está inicializada
        inicializar_app()
        logger.info("Iniciando servidor Flask em modo desenvolvimento")
        app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
    except Exception as e:
        logger.error(f"ERRO CRÍTICO ao iniciar servidor: {str(e)}")
        sys.exit(1)