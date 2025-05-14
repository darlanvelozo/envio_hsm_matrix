# Processador de Faturas Vencidas

Aplicativo web para processar arquivos CSV contendo códigos de clientes, buscar faturas vencidas através da API e gerar um novo CSV com as informações de contato e pagamento.

## Funcionalidades

- Processamento de arquivos CSV com códigos de clientes
- Busca de faturas vencidas mais antigas para cada cliente
- Geração de CSV com informações para contato e pagamento
- Processamento em lotes para lidar com grandes volumes de dados
- Interface web simples e intuitiva

## Estrutura do CSV de Saída

O CSV gerado contém as seguintes colunas:

- `Telefone` - Número de telefone do cliente (quando disponível)
- `Nome` - Nome completo do cliente
- `1` - Nome do cliente
- `2` - Valor da fatura
- `3` - Data de vencimento
- `4` - Código de barras
- `5` - PIX copia e cola
- `6` - Link do boleto

## Requisitos

- Python 3.7+
- Flask 2.0.1
- Werkzeug 2.0.1
- Pandas 1.3.5
- Requests 2.27.1
- NumPy 1.21.6

## Instalação

1. Clone este repositório ou baixe os arquivos
2. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Execução

1. Execute o aplicativo Flask:

```bash
python app.py
```

2. Acesse o aplicativo em seu navegador através do endereço: `http://127.0.0.1:5000`

## Como usar

1. Prepare um arquivo CSV contendo pelo menos uma coluna chamada `codigo_cliente` com os códigos dos clientes
2. Se disponível, inclua uma coluna chamada `TelefoneCorrigido` com os números de telefone
3. Faça upload do arquivo CSV na interface web
4. Aguarde o processamento (pode demorar alguns minutos para arquivos grandes)
5. O download do arquivo CSV com os resultados será iniciado automaticamente

## Processamento em Lotes

Para arquivos com muitos clientes, o sistema processa os dados em lotes para evitar sobrecarregar a API e garantir que todos os registros sejam processados corretamente:

- Os clientes são processados em lotes de 10 por vez
- Cada lote utiliza até 5 threads paralelas para otimizar o tempo de processamento
- Intervalos de espera são inseridos entre os lotes para evitar rate limiting
- O sistema implementa retry com backoff exponencial para lidar com falhas temporárias

## Limitações

- O processamento de arquivos muito grandes (mais de 500 clientes) pode exceder o timeout do navegador
- Em caso de timeout, divida o arquivo CSV em partes menores
- Se o processamento falhar no meio, a aplicação não salva o progresso parcial

## Versão Offline

Para processamento offline (sem interface web), você pode usar a versão CLI disponível em `main.py`:

```bash
python main.py
```

Esta versão oferece:
- Processamento de cliente único ou arquivo CSV
- Salvamento de resultados em disco local
- Mesmas funcionalidades de processamento em lotes e retry 