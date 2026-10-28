# importando as bibliotecas necessárias
import json
import pika
import time
import sys
import os
import duckdb

# Garante que a raiz do projeto esteja no path para localizar o pacote config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import (
    RABBITMQ_HOST, 
    RABBITMQ_PORT, 
    RABBITMQ_USER, 
    RABBITMQ_PASSWORD, 
    QUEUE_TRANSACOES, 
    DUCKDB_PATH
)

def inicializar_banco_bronze():
    """garante qua a pasta dse dados e a tabela Bronze existam"""
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    
    # Cria a tabela Bronze se ela não existir
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_transacoes (
            payload JSON,
            processado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    con.close()
    print(f"✅ [DUCKDB] Banco de dados Bronze inicializado em '{DUCKDB_PATH}' e tabela 'bronze_transacoes' criada (se não existia).")
    
# Função para conectar ao RabbitMQ com retry e fila durável
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
            

def iniciar_consumidor_bronze():
    """Inicia o consumidor que lê mensagens da fila e grava no banco Bronze"""
    # Inicializa o banco Bronze
    inicializar_banco_bronze()
    
    # Conecta ao RabbitMQ
    connection, channel = conectar_rabbitmq()
    
    # ELASTICIDADE: Garante que o RabbitMQ distribua apenas 1 mensagem por vez 
    # para cada worker ativo, permitindo múltiplos terminais rodando em paralelo.
    channel.basic_qos(prefetch_count=1)
    
    
    def callback(ch, method, properties, body):
        """Callback para processar cada mensagem recebida da fila"""
        try:
            # Converte a mensagem JSON para dicionário
            transacao = json.loads(body)
            
            # Conecta ao banco Bronze e insere a transação
            con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
            con.execute("INSERT INTO bronze_transacoes (payload) VALUES (?)", [json.dumps(transacao)])
            con.close()
            
            print(f"✅ [BRONZE] Transação gravada no banco Bronze: {transacao}")
            
            # Confirma que a mensagem foi processada com sucesso
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"❌ [ERRO] Falha ao processar a mensagem: {e}")
            # Não confirma a mensagem, permitindo reprocessamento futuro
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    # Inicia o consumo da fila
    channel.basic_consume(queue=QUEUE_TRANSACOES,
                          on_message_callback=callback)
    
    print(f"🚀 [CONSUMIDOR BRONZE] Aguardando mensagens na fila '{QUEUE_TRANSACOES}'... Pressione Ctrl+C para sair.")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("🛑 [CONSUMIDOR BRONZE] Interrompido pelo usuário. Encerrando...")
        channel.stop_consuming()
        
if __name__ == "__main__":
    iniciar_consumidor_bronze()
    
    
    

  