# src/consumer_silver.py
import json
import os
import sys
import duckdb
from datetime import datetime

# Garante que a raiz du projeto esteja no path para localizar o pacote config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import DUCKDB_PATH

def inicializar_banco_silver():
    """Garante que a tabela Silver exista no DuckDB com tipagem estrita e schema compatível"""
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    
    # Criação da tabela Silver alinhada com as colunas reais do arquivo Parquet
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_transacoes (
            trans_num VARCHAR,
            cc_num BIGINT,
            merchant VARCHAR,
            category VARCHAR,
            amt DOUBLE,
            nome_cliente VARCHAR,
            cidade VARCHAR,
            estado VARCHAR,
            is_fraud INTEGER,
            trans_date_trans_time TIMESTAMP,
            processado_silver_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    con.close()
    print("Camada Silver inicializada e tabela 'silver_transacoes' pronta.")

def transformar_e_carregar_silver():
    """Lê os dados crus da Bronze, limpa, tipifica com base nas colunas reais e grava na Silver"""
    inicializar_banco_silver()
    
    print("Iniciando processamento de transformação da Camada Bronze para a Silver...")
    
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    
    query = "SELECT payload FROM bronze_transacoes;"
    df_bronze = con.execute(query).fetchdf()
    
    sucessos = 0
    erros = 0

    for index, row in df_bronze.iterrows():
        try:
            # Converte o payload JSON armazenado na bronze em dicionário Python
            dados = json.loads(row['payload'])
            
          
            trans_num = str(dados.get('trans_num', f'unknown_{index}'))
            
            try:
                cc_num = int(dados.get('cc_num', 0))
            except (ValueError, TypeError):
                cc_num = 0

            merchant = str(dados.get('merchant', 'Desconhecido'))
            category = str(dados.get('category', 'geral'))

            # Tratamento numérico seguro para o valor da transação (amt)
            try:
                amt = float(dados.get('amt', 0.0))
            except (ValueError, TypeError):
                amt = 0.0

            # Nome completo combinando 'first' e 'last' do dataset real
            primeiro_nome = str(dados.get('first', ''))
            ultimo_nome = str(dados.get('last', ''))
            nome_cliente = f"{primeiro_nome} {ultimo_nome}".strip() or "N/A"

            cidade = str(dados.get('city', 'N/A'))
            estado = str(dados.get('state', 'N/A'))

            # Tratamento do status de fraude (is_fraud)
            try:
                is_fraud = int(dados.get('is_fraud', 0))
            except (ValueError, TypeError):
                is_fraud = 0

            # Tratamento de data/timestamp (trans_date_trans_time)
            trans_date = dados.get('trans_date_trans_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            trans_date_trans_time = str(trans_date)

            # Insere os dados limpos e tipados na tabela Silver
            con.execute("""
                INSERT INTO silver_transacoes (
                    trans_num, cc_num, merchant, category, amt, 
                    nome_cliente, cidade, estado, is_fraud, trans_date_trans_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [
                trans_num, cc_num, merchant, category, amt, 
                nome_cliente, cidade, estado, is_fraud, trans_date_trans_time
            ])
            
            sucessos += 1

        except Exception as e:
            erros += 1
            print(f"Falha ao transformar registro {index}: {e}")

    con.close()
    print(f"Processamento concluído! Sucessos: {sucessos} | Registros com erro ignorados: {erros}")

if __name__ == "__main__":
    transformar_e_carregar_silver()