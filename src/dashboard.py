import pandas as pd
import streamlit as st
from config.settings import DUCKDB_PATH
import duckdb


# Configuação da página do Streamlit
st.set_page_config(
    page_title="Monitor Antifraude",
    page_icon=":bar_chart:",
    layout="wide"
)


st.title("Painel de Monitoramento Antifraude em Tempo Real")
st.markdown('Plataforma de Engenharia de Dados orientada a eventos (Docker, RabbitMQ, DuckDB e Feature Store).')

# Conexão com o DuckDB
def carregar_dados(query):
    conn = duckdb.connect(database = DUCKDB_PATH, read_only = True)
    df = conn.execute(query).df()
    conn.close()
    return df

# Layout em Métricas KPI
col1, col2, col3 = st.columns(3)

try:
    df_clientes = carregar_dados("SELECT COUNT(*) as total, AVG(ticket_medio) as ticket FROM gold_features_cliente;")
    df_alertas = carregar_dados("SELECT COUNT(*) as total FROM gold_alertas_fraude;")
    
    with col1:
        st.metric("Total de Clientes Monitorados", int(df_clientes['total'].iloc[0]))
    with col2:
        st.metric("Ticket Médio Geral", f"R$ {df_clientes['ticket'].iloc[0]:.2f}")
    with col3:
        st.metric("Alertas de Risco Gerados", int(df_alertas['total'].iloc[0]))

    st.markdown("---")
    
    # Tabela de Alertas de Fraude em Tempo Real
    st.subheader("Últimos Alertas e Transações de Risco (Camada Gold)")
    df_tabela_alertas = carregar_dados("SELECT * FROM gold_alertas_fraude LIMIT 10;")
    st.dataframe(df_tabela_alertas, use_container_width=True)
    
    # Gráfico de Faturamento por Categoria
    st.subheader("Faturamento e Volume por Categoria de Comércio")
    df_categorias = carregar_dados("SELECT category, SUM(faturamento_total) as faturamento FROM gold_fato_categoria GROUP BY category;")
    st.bar_chart(df_categorias.set_index('category'))

except Exception as e:
    st.warning("As tabelas da Camada Gold ainda não foram populadas. Execute os scripts consumers (`consumer_silver.py` e `consumer_gold.py`) primeiro.")

