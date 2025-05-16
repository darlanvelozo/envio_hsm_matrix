#!/usr/bin/env python
"""
Script para verificar o ambiente do Gunicorn e detectar possíveis problemas.
Execute este script da mesma forma que o Gunicorn é executado.
"""

import os
import sys
import logging
import json
import traceback

def check_environment():
    """Verifica o ambiente e retorna um dicionário com informações relevantes."""
    info = {
        "python_version": sys.version,
        "python_path": sys.executable,
        "current_dir": os.getcwd(),
        "script_dir": os.path.dirname(os.path.abspath(__file__)),
        "user": os.getenv("USER") or "unknown",
        "path_env": os.getenv("PATH") or "not set",
        "pythonpath": os.getenv("PYTHONPATH") or "not set",
        "permissions": {},
        "directory_contents": {}
    }
    
    # Verificar permissões dos diretórios importantes
    for dirname in ['temp_csv', 'logs', 'templates', 'cache']:
        dirpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), dirname)
        info["permissions"][dirname] = {
            "exists": os.path.exists(dirpath),
            "is_dir": os.path.isdir(dirpath) if os.path.exists(dirpath) else False,
            "readable": os.access(dirpath, os.R_OK) if os.path.exists(dirpath) else False,
            "writable": os.access(dirpath, os.W_OK) if os.path.exists(dirpath) else False,
            "executable": os.access(dirpath, os.X_OK) if os.path.exists(dirpath) else False
        }
        
        # Listar conteúdo do diretório
        if os.path.isdir(dirpath) and os.access(dirpath, os.R_OK):
            try:
                info["directory_contents"][dirname] = os.listdir(dirpath)
            except Exception as e:
                info["directory_contents"][dirname] = f"Erro ao listar: {str(e)}"
    
    # Verificar módulos importados
    try:
        import flask
        info["flask_version"] = flask.__version__
    except ImportError:
        info["flask_version"] = "não instalado"
        
    try:
        import gunicorn
        info["gunicorn_version"] = gunicorn.__version__
    except ImportError:
        info["gunicorn_version"] = "não instalado"
    
    try:
        import pandas
        info["pandas_version"] = pandas.__version__
    except ImportError:
        info["pandas_version"] = "não instalado"
    
    # Testar se pode criar arquivos nos diretórios importantes
    for dirname in ['temp_csv', 'logs', 'cache']:
        dirpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), dirname)
        if os.path.isdir(dirpath) and os.access(dirpath, os.W_OK):
            test_file = os.path.join(dirpath, "test_write.txt")
            try:
                with open(test_file, 'w') as f:
                    f.write("Test write")
                info["permissions"][f"{dirname}_write_test"] = "success"
                # Remover o arquivo de teste
                try:
                    os.remove(test_file)
                except:
                    info["permissions"][f"{dirname}_cleanup"] = "failed"
            except Exception as e:
                info["permissions"][f"{dirname}_write_test"] = f"failed: {str(e)}"
    
    return info

def main():
    """Função principal que executa as verificações e exibe os resultados."""
    print("Verificando ambiente para diagnóstico...")
    
    try:
        info = check_environment()
        
        # Salvar resultados em um arquivo JSON
        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs/environment_check.json')
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(info, f, indent=2)
        
        print(f"Resultados salvos em {output_file}")
        
        # Exibir informações críticas
        print("\n=== INFORMAÇÕES CRÍTICAS ===")
        print(f"Versão Python: {info['python_version']}")
        print(f"Diretório atual: {info['current_dir']}")
        print(f"Diretório do script: {info['script_dir']}")
        print(f"Usuário: {info['user']}")
        
        print("\n=== VERIFICAÇÃO DE PERMISSÕES ===")
        for dirname, perms in info["permissions"].items():
            if "_write_test" not in dirname and "_cleanup" not in dirname:
                status = "OK" if perms.get("writable", False) else "FALHA"
                print(f"{dirname}: {status}")
        
        print("\n=== VERSÕES DE PACOTES ===")
        print(f"Flask: {info.get('flask_version', 'não verificado')}")
        print(f"Gunicorn: {info.get('gunicorn_version', 'não verificado')}")
        print(f"Pandas: {info.get('pandas_version', 'não verificado')}")
        
        print("\nPara mais detalhes, consulte o arquivo JSON gerado.")
    
    except Exception as e:
        print(f"Erro durante a verificação: {str(e)}")
        print(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 