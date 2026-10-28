#Configuração do RabbitMQ

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'admin123'

#Nome da fila para envio de mensagens
QUEUE_TRANSACOES= 'fila_transacoes'

#Caminho dos dados e do banco de dados DuckDB
DUCKDB_PATH = 'data/antifraude.db'
PARQUET_FRAUDES_PATH = 'data/Fraudes.parquet'
PARQUET_TRANSACOES_PATH = 'data/fraudesFinanceira.parquet'