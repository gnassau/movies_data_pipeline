# 🎬 Snowflake Movies Pipeline

> Pipeline de dados end-to-end que ingere filmes da API TMDB, armazena no Amazon S3 e transforma os dados no Snowflake usando dbt — tudo orquestrado pelo Apache Airflow rodando em Docker.

---

## 🎯 Objetivo

Construir um pipeline de dados completo para análise de filmes, cobrindo desde a extração da API até tabelas analíticas prontas para consumo. O pipeline suporta tanto carga completa quanto atualização incremental, permitindo manter os dados sempre atualizados com o mínimo de reprocessamento.

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| **Apache Airflow** | 2.9.1 | Orquestração do pipeline |
| **Snowflake** | — | Data Warehouse |
| **dbt** | 1.7.9 | Transformação de dados (Silver e Gold) |
| **dbt-snowflake** | 1.7.4 | Adapter dbt para Snowflake |
| **Amazon S3** | — | Data Lake (camada Bronze raw) |
| **Docker / Docker Compose** | — | Ambiente containerizado |
| **Python** | 3.12 | Scripts de ingestão |
| **AWS Wrangler** | 3.7.2 | Escrita de dados no S3 |
| **TMDB API** | — | Fonte dos dados de filmes |

---

## 🏗️ Arquitetura

```
┌─────────────┐     ┌───────────┐     ┌──────────────────────────────────────┐
│  TMDB API   │────▶│  Python   │────▶│           Amazon S3                  │
│             │     │ (Bronze)  │     │     s3://bucket/bronze/...           │
└─────────────┘     └───────────┘     └──────────────┬───────────────────────┘
                                                      │ COPY INTO
                                                      ▼
                                      ┌──────────────────────────────────────┐
                                      │           Snowflake                  │
                                      │                                      │
                                      │  BRONZE          (raw JSON/Variant)  │
                                      │    └── MOVIES                        │
                                      │    └── MOVIES_UPDATES                │
                                      │    └── UPDATED_MOVIE_IDS             │
                                      │                                      │
                                      │  SILVER          (dbt, tabelas limpas)│
                                      │    └── movies                        │
                                      │    └── movies_updates                │
                                      │    └── updated_movie_ids             │
                                      │                                      │
                                      │  GOLD            (dbt, analíticas)   │
                                      │    └── best_movies                   │
                                      │    └── most_profitable_movies        │
                                      │    └── high_engagement_movies        │
                                      │    └── general_performance           │
                                      │    └── movies_by_year                │
                                      └──────────────────────────────────────┘
```

Todo o pipeline é orquestrado pelo **Airflow**, rodando containerizado via Docker Compose.

---

## 📁 Estrutura do Projeto

```
Snowflake_movies_project_v2/
│
├── src/                          # Código Python de ingestão
│   ├── Bronze/
│   │   ├── full_load.py          # Ingestão completa da API TMDB
│   │   ├── get_updated_ids.py    # Busca IDs de filmes atualizados
│   │   └── update_movies.py      # Busca detalhes dos filmes atualizados
│   └── sql/
│       └── Snowflake_Setup/      # Scripts SQL de infraestrutura
│           ├── 00_create_warehouse.sql
│           ├── 01_create_database.sql
│           ├── 02_create_schemas.sql
│           ├── 03_create_bronze_tables.sql
│           ├── 04_storage_integration.sql
│           └── 05_copy_into.sql
│
├── airflow/                      # Orquestração
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── .env                      # Variáveis de ambiente (não versionar)
│   └── dags/
│       ├── snowflake_setup.py    # DAG de setup inicial (roda uma vez)
│       ├── snowflake_ingest.py   # DAG principal do pipeline (diário)
│       └── test_snowflake_connection.py
│
└── movies_dbt/                   # Transformações dbt
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── bronze/
        │   └── bronze.yml        # Declara sources da camada Bronze
        ├── silver/
        │   ├── silver.yml
        │   ├── movies.sql
        │   ├── movies_updates.sql
        │   └── updated_movie_ids.sql
        └── gold/
            ├── gold_tables_tests.yml
            ├── best_movies.sql
            ├── most_profitable_movies.sql
            ├── high_engagement_movies.sql
            ├── general_performance.sql
            └── movies_by_year.sql
```

---

## 🔄 DAGs do Airflow

### `snowflake_setup` — Setup inicial
Deve ser executada **uma única vez** antes de iniciar o pipeline. Cria toda a infraestrutura no Snowflake em ordem:

```
create_warehouse → create_database → create_schemas → create_bronze_tables → create_storage_integration
```

### `snowflake_ingest` — Pipeline diário
Executa automaticamente todo dia e suporta dois modos de carga configuráveis via parâmetro:

| Parâmetro | Opções | Descrição |
|---|---|---|
| `load_type` | `incremental_refresh` / `full_refresh` | Tipo de ingestão |
| `start_date` | `YYYY-MM-DD` | Data inicial (opcional) |
| `end_date` | `YYYY-MM-DD` | Data final (opcional) |

Fluxo de execução:

```
bronze_layer
  └── full_load → get_updated_ids → get_movie_details
        │
        ▼
copy_into_snowflake
        │
        ▼
silver_layer
  └── dbt run → dbt test
        │
        ▼
gold_layer
  └── dbt run → dbt test
```

---

## 📊 Tabelas Gold

| Tabela | Descrição |
|---|---|
| `best_movies` | Filmes com maior média de avaliação (mínimo 100 votos) |
| `most_profitable_movies` | Filmes ordenados por lucro (receita − orçamento) |
| `high_engagement_movies` | Filmes com maior popularidade e engajamento |
| `general_performance` | Métricas gerais por filme: rating, receita, orçamento e lucro médio |
| `movies_by_year` | Quantidade de filmes lançados por ano |

---

## ⚙️ Como Configurar

### Pré-requisitos

- Docker e Docker Compose instalados
- Conta na [TMDB API](https://www.themoviedb.org/documentation/api) com chave de acesso
- Conta AWS com um bucket S3 e uma role IAM configurada para o Snowflake
- Conta Snowflake

### 1. Variáveis de ambiente

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp .env_example airflow/.env
```

Edite `airflow/.env`:

```env
AIRFLOW_UID=1000

# PostgreSQL (backend do Airflow)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Snowflake
SNOWFLAKE_USER=seu_usuario
SNOWFLAKE_PASSWORD=sua_senha
SNOWFLAKE_ACCOUNT=seu_account_id
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 2. Subir os containers

```bash
cd airflow
docker-compose up -d --build
```

Aguarde todos os serviços ficarem saudáveis. O Airflow estará disponível em `http://localhost:8080` (usuário e senha padrão: `airflow`).

### 3. Configurar conexão Snowflake no Airflow

Na interface do Airflow, vá em **Admin → Connections** e crie uma conexão:

| Campo | Valor |
|---|---|
| Connection ID | `snowflake_default` |
| Connection Type | `Snowflake` |
| Account | seu account ID |
| Login | seu usuário |
| Password | sua senha |
| Warehouse | `COMPUTE_WH` |
| Role | `ACCOUNTADMIN` |

### 4. Configurar Airflow Variables

Em **Admin → Variables**, adicione as seguintes variáveis necessárias para a storage integration com S3:

| Chave | Descrição |
|---|---|
| `STORAGE_AWS_ROLE_ARN` | ARN da role IAM do Snowflake |
| `STORAGE_ALLOWED_LOCATIONS` | URLs S3 permitidas (separadas por vírgula) |
| `S3_BRONZE_URL` | URL do bucket S3 Bronze |
| `SNOWFLAKE_USER` | Usuário Snowflake |
| `SNOWFLAKE_PASSWORD` | Senha Snowflake |
| `SNOWFLAKE_ACCOUNT` | Account ID Snowflake |

### 5. Executar o setup inicial

Na interface do Airflow, ative e execute manualmente a DAG `snowflake_setup`. Ela criará toda a infraestrutura no Snowflake (warehouse, database, schemas, tabelas e storage integration).

### 6. Executar o pipeline

Ative a DAG `snowflake_ingest`. Ela roda automaticamente todo dia, ou pode ser disparada manualmente com os parâmetros desejados.

---

## 🔐 Segurança

- Nunca versione o arquivo `airflow/.env` — ele já está no `.gitignore`
- Use roles com o mínimo de privilégios necessários no Snowflake para ambientes de produção
- Rotacione a chave da TMDB API periodicamente
