# 🛡️ Plataforma de Engenharia de Dados Antifraude em Tempo Real

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.4-orange?logo=rabbitmq&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow?logo=duckdb&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)

> **Projeto Aplicado — Pós-Graduação em Engenharia de Dados (XPE)**  
> Arquitetura orientada a eventos (*Event-Driven Architecture*) para ingestão, processamento em *streaming* e detecção de fraudes financeiras em tempo real utilizando **Docker**, **RabbitMQ**, **DuckDB** e **Parquet**.

---

## 🎯 Problema e Objetivo

Instituições financeiras enfrentam perdas bilionárias com fraudes instantâneas (como o Pix) devido à falta de integração entre os dados de cibersegurança (SOC) e o fluxo transacional bancário, além da alta latência dos sistemas legados em lote (*batch*).

* **Objetivo S.M.A.R.T:** Construir um pipeline de engenharia de dados leve, resiliente e *open-source* capaz de ingerir, cruzar e disponibilizar indicadores de risco em uma *Feature Store* com **latência inferior a 1 segundo** (sub-segundo).

---

## 🏗️ Arquitetura da Solução (Medallion Architecture)

O pipeline opera de forma assíncrona e orientada a eventos, estruturado nas seguintes camadas:

```text
[Dados Parquet] ──► [Producer.py] ──► (RabbitMQ Broker)
                                              │
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        ▼                                     ▼                                     ▼
[Consumer Bronze]                     [Consumer Silver]                     [Consumer Gold]
(Persistência Bruta)                  (Limpeza e Tipagem)                   (Feature Store & Regras)
        │                                     │                                     │
        └─────────────────────────────────────┴─────────────────────────────────────┘
                                              │
                                              ▼
                                    [DuckDB Local OLAP] ──► [Streamlit Dashboard]
```

1. **Ingestão (`producer.py`):** Lê eventos transacionais em formato `.parquet` via DuckDB em memória e publica na fila durável do RabbitMQ.
2. **Camada Bronze (`consumer_bronze.py`):** Consome mensagens com controle de fluxo (`prefetch_count=1`) e confirmação manual (`ACK`), garantindo persistência atômica e zero perda de dados.
3. **Camada Silver (`consumer_silver.py`):** Realiza higienização, padronização de esquemas e tipagem estrita com tratamento granular de exceções.
4. **Camada Gold (`consumer_gold.py`):** Consolida métricas comportamentais por cliente (`gold_features_cliente`, ticket médio, histórico de fraudes) sob isolamento transacional (`BEGIN TRANSACTION` / `COMMIT`).
5. **Observabilidade (`dashboard.py` & `benchmark.py`):** Painel analítico interativo em tempo real e script de validação empírica de latência.

---

## 📂 Estrutura do Repositório

```text
├── config/
│   └── settings.py            # Credenciais, hosts, nomes de filas e caminhos de arquivos
├── data/
│   └── transacoes.parquet     # Dataset analítico de transações financeiras (sintético/LGPD)
├── src/
│   ├── producer.py            # Simulador de streaming de eventos para o RabbitMQ
│   ├── consumer_bronze.py     # Ingestão bruta e atômica no DuckDB
│   ├── consumer_silver.py     # Limpeza, padronização e tipagem de dados
│   ├── consumer_gold.py       # Construção da Feature Store e regras de alerta
│   ├── benchmark.py           # Teste de performance e validação da meta SMART (< 1s)
│   └── dashboard.py           # Interface visual em Streamlit para monitoramento em tempo real
├── docker-compose.yml         # Orquestração do contêiner RabbitMQ com Management UI
├── requirements.txt           # Dependências Python do projeto
└── README.md                  # Documentação do projeto
```

---

## 🚀 Como Executar o Projeto

### 1. Pré-requisitos
* **Docker** e **Docker Compose** instalados.
* **Python 3.10+** instalado.

### 2. Subir o Barramento de Mensageria (RabbitMQ)
Na raiz do projeto, execute o Docker Compose para iniciar o RabbitMQ:
```bash
docker-compose up -d
```
* **Painel Web de Gerenciamento:** Acesse `http://localhost:15672` (Usuário padrão: `admin` / Senha: definida no `.env` ou `docker-compose.yml`).

### 3. Configurar o Ambiente Python
Crie um ambiente virtual e instale as dependências:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou: venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 4. Executar o Pipeline em Streaming
Abra terminais separados para acompanhar o fluxo assíncrono em tempo real:

* **Terminal 1 — Iniciar os Consumidores (Camadas Bronze, Silver e Gold):**
```bash
python src/consumer_bronze.py
python src/consumer_silver.py
python src/consumer_gold.py
```

* **Terminal 2 — Disparar a Ingestão de Eventos (Produtor):**
```bash
python src/producer.py
```

### 5. Validar Latência e Abrir o Dashboard
* **Executar Benchmark de Desempenho:**
```bash
python src/benchmark.py
```
* **Iniciar o Painel de Monitoramento (Streamlit):**
```bash
streamlit run src/dashboard.py
```
Acesse no navegador: `http://localhost:8501`

---

## 📊 Resultados e Benchmarks Validados

* **Latência Média na Camada Gold:** **`0.0297 segundos`** (~29,7 ms) para agregação analítica de registros, superando com ampla margem a meta S.M.A.R.T estabelecida (`< 1.0000s`).
* **Resiliência:** Uso de filas duráveis (`durable=True`), persistência em disco (`delivery_mode=2`) e controle de carga (`prefetch_count=1`).

---

## 🔮 Próximos Passos (Roadmap)
* Integração com APIs nativas e *webhooks* de pagamentos instantâneos (Pix) e *logs* reais de SOC.
* Migração para arquitetura em nuvem (AWS/GCP) com **Kubernetes** e **Apache Kafka** para escala horizontal massiva.
* Acoplamento de modelos preditivos de Machine Learning (XGBoost/LightGBM) servidos via API sobre a *Feature Store*.