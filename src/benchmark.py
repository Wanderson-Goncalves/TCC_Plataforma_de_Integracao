# src/benchmark.py
import os
import sys
import time
import duckdb

# Garante que a raiz do projeto esteja no path para localizar o pacote config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import DUCKDB_PATH

def medir_latencia_e_desempenho():
    print("Iniciando medição de latência e testes de desempenho da Camada Gold...")
    
    inicio = time.time()
    
    # Conecta ao DuckDB para consultar o tempo de processamento dos registros
    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
    
    # Consulta a tabela de alertas da Gold para verificar a rapidez e volumetria
    total_alertas = con.execute("SELECT COUNT(*) FROM gold_alertas_fraude;").fetchone()[0]
    
    fim = time.time()
    tempo_consulta = fim - inicio
    
    print(f"Total de registros analisados na Gold: {total_alertas}")
    print(f"Tempo de resposta do motor OLAP (DuckDB): {tempo_consulta:.4f} segundos")
    
    # Validação da Meta SMART (Inferior a 1 segundo)
    if tempo_consulta < 1.0:
        print("Sucesso! O tempo de resposta está estritamente abaixo da meta de 1 segundo (< 1s).")
    else:
        print("O tempo excedeu o limite planejado.")
        
    con.close()

if __name__ == "__main__":
    medir_latencia_e_desempenho()