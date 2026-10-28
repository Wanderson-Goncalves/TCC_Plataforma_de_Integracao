# src/producer.py
import json
import pika
import time
import duckdb
import sys
import os

# Garante que a raiz do projeto esteja no path para localizar o pacote config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import (
    RABBITMQ_HOST, 
    RABBITMQ_PORT, 
    RABBITMQ_USER, 
    RABBITMQ_PASSWORD, 
    QUEUE_TRANSACOES, 
    PARQUET_TRANSACOES_PATH
)

def conectar_rabbitmq():
    """Estabelece conexão resiliente com o RabbitMQ com política de retry e fila durável"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            # Fila durável (durable=True) garante que ela sobreviva a reinícios do broker
            channel.queue_declare(queue=QUEUE_TRANSACOES, durable=True)
            print("✅ [RABBITMQ] Conexão estabelecida com sucesso!")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"⚠️ [RABBITMQ] Erro ao conectar: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)

def transmitir_parquet_para_streaming():
    """Lê dados do Parquet via DuckDB e transmite como eventos de streaming para o RabbitMQ"""
    # Valida se o arquivo de transações financeiras existe antes de iniciar o banco
    if not os.path.exists(PARQUET_TRANSACOES_PATH):
        print(f"❌ ATENÇÃO: O arquivo de transações '{PARQUET_TRANSACOES_PATH}' não foi encontrado.")
        return

    print(f"📊 [DUCKDB] Lendo dados analíticos do arquivo Parquet: {PARQUET_TRANSACOES_PATH}...")
    
    # Conecta em memória (:memory:) evita travar arquivos locais se múltiplos scripts rodarem
    con = duckdb.connect(database=':memory:')
    
    # LIMIT 100 ideal para testes rápidos/Sprints. Remova o "LIMIT 100" para carga total.
    query = f"SELECT * FROM read_parquet('{PARQUET_TRANSACOES_PATH}') LIMIT 100;"
    df = con.execute(query).fetchdf()
    con.close()

    # Inicializa conexões do RabbitMQ
    connection, channel = conectar_rabbitmq()

    print(f"🚀 [PRODUCER] Iniciando transmissão em streaming para a fila '{QUEUE_TRANSACOES}'...")

    for index, row in df.iterrows():
        transacao = row.to_dict()
        
        # Converte tipos complexos (ex: Timestamp do Pandas/Numpy) para strings aceitas pelo JSON puro
        transacao_serializavel = {
            k: (str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v) 
            for k, v in transacao.items()
        }

        # Publica com delivery_mode=2 para persistência da mensagem em disco
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_TRANSACOES,
            body=json.dumps(transacao_serializavel),
            properties=pika.BasicProperties(
                delivery_mode=2  
            )
        )
        
        print(f"📤 [STREAMING] Transação enviada -> Índice: {index}")
        time.sleep(0.1)  # Simula o intervalo de chegada de transações (10 por segundo)

    connection.close()
    print("✅ [PRODUCER] Transmissão concluída e conexão fechada com sucesso!")

if __name__ == "__main__":
    transmitir_parquet_para_streaming()
