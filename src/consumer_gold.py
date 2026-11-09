
import os
import sys
import duckdb
from datetime import datetime

# Garante que a raiz do projeto esteja no path para localizar o pacote config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import DUCKDB_PATH

def construir_camada_gold():
    """
    Constrói a Camada Gold (Feature Store e Tabelas Analíticas) de forma robusta,
    idempotente e otimizada para Cientistas de Dados e Analistas de BI.
    """
    print("Iniciando a construção avançada da Feature Store e Tabelas Gold...")
    
    # Conexão com o banco DuckDB analítico
    con = duckdb.connect(database=DUCKDB_PATH, read_only=False)
    
    try:
        # Inicia uma transação segura
        con.execute("BEGIN TRANSACTION;")

        # =====================================================================
        # 1. FEATURE STORE POR CLIENTE (gold_features_cliente)
        # Agregados de risco comportamental em tempo real baseados no cartão/cliente
        # =====================================================================
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold_features_cliente (
                cc_num BIGINT PRIMARY KEY,
                total_transacoes BIGINT,
                valor_total_gasto DOUBLE,
                ticket_medio DOUBLE,
                maior_transacao DOUBLE,
                menor_transacao DOUBLE,
                total_fraudes_historico BIGINT,
                taxa_fraude_cliente DOUBLE,
                ultima_transacao TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Limpa e recarrega a tabela de features 
        con.execute("DELETE FROM gold_features_cliente;")
        con.execute("""
            INSERT INTO gold_features_cliente (
                cc_num, total_transacoes, valor_total_gasto, ticket_medio, 
                maior_transacao, menor_transacao, total_fraudes_historico, 
                taxa_fraude_cliente, ultima_transacao
            )
            SELECT 
                cc_num,
                COUNT(trans_num) AS total_transacoes,
                SUM(amt) AS valor_total_gasto,
                AVG(amt) AS ticket_medio,
                MAX(amt) AS maior_transacao,
                MIN(amt) AS menor_transacao,
                SUM(is_fraud) AS total_fraudes_historico,
                (CAST(SUM(is_fraud) AS DOUBLE) / NULLIF(COUNT(trans_num), 0)) * 100 AS taxa_fraude_cliente,
                MAX(trans_date_trans_time) AS ultima_transacao
            FROM silver_transacoes
            GROUP BY cc_num;
        """)
        print("Feature Store de Clientes (`gold_features_cliente`) populada com sucesso.")

        # =====================================================================
        # 2. FATO E DIMENSÃO DE CATEGORIAS E ESTABELECIMENTOS (gold_fato_categoria)
        # Visão analítica para relatórios de fraude por merchant e categoria de consumo
        # =====================================================================
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold_fato_categoria (
                category VARCHAR,
                merchant VARCHAR,
                qtd_transacoes BIGINT,
                faturamento_total DOUBLE,
                media_valor_categoria DOUBLE,
                fraudes_na_categoria BIGINT,
                taxa_fraude_merchant DOUBLE,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        con.execute("DELETE FROM gold_fato_categoria;")
        con.execute("""
            INSERT INTO gold_fato_categoria (
                category, merchant, qtd_transacoes, faturamento_total, 
                media_valor_categoria, fraudes_na_categoria, taxa_fraude_merchant
            )
            SELECT 
                category,
                merchant,
                COUNT(trans_num) AS qtd_transacoes,
                SUM(amt) AS faturamento_total,
                AVG(amt) AS media_valor_categoria,
                SUM(is_fraud) AS fraudes_na_categoria,
                (CAST(SUM(is_fraud) AS DOUBLE) / NULLIF(COUNT(trans_num), 0)) * 100 AS taxa_fraude_merchant
            FROM silver_transacoes
            GROUP BY category, merchant;
        """)
        print("Tabela Analítica de Fato (`gold_fato_categoria`) populada com sucesso.")

        # =====================================================================
        # 3. TABELA DE ALERTAS EM TEMPO REAL PARA ANALISTAS (gold_alertas_fraude)
        # Filtra transações suspeitas ou de alto valor para monitoramento imediato
        # =====================================================================
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold_alertas_fraude (
                trans_num VARCHAR,
                cc_num BIGINT,
                nome_cliente VARCHAR,
                merchant VARCHAR,
                category VARCHAR,
                amt DOUBLE,
                cidade VARCHAR,
                estado VARCHAR,
                trans_date_trans_time TIMESTAMP,
                nivel_risco VARCHAR,
                processado_gold_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        con.execute("DELETE FROM gold_alertas_fraude;")
        con.execute("""
            INSERT INTO gold_alertas_fraude (
                trans_num, cc_num, nome_cliente, merchant, category, 
                amt, cidade, estado, trans_date_trans_time, nivel_risco
            )
            SELECT 
                trans_num,
                cc_num,
                nome_cliente,
                merchant,
                category,
                amt,
                cidade,
                estado,
                trans_date_trans_time,
                CASE 
                    WHEN is_fraud = 1 OR amt > 500.0 THEN 'ALTO RISCO'
                    WHEN amt > 200.0 THEN 'MÉDIO RISCO'
                    ELSE 'BAIXO RISCO'
                END AS nivel_risco
            FROM silver_transacoes
            WHERE is_fraud = 1 OR amt > 150.0;
        """)
        print("Tabela de Monitoramento de Alertas (`gold_alertas_fraude`) criada com sucesso.")

        # Efetiva a transação no banco de dados
        con.execute("COMMIT;")
        print("Camada Gold construída integralmente!")

    except Exception as e:
        # Em caso de qualquer erro, desfaz a transação garantindo consistência
        con.execute("ROLLBACK;")
        print(f"Falha crítica ao construir a Camada Gold: {e}")
        raise e
        
    finally:
        con.close()

if __name__ == "__main__":
    construir_camada_gold()