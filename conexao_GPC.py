from google.cloud import bigquery
import os
import pandas as pd
import sys
import logging
from datetime import datetime, timezone

# ---------------------
# CONFIGURAÇÕES
# ---------------------

PROJECT_ID = "rodrigo-ebac-503417"
TABLE_ID = "rodrigo-ebac-503417.london_crimes.crime_by_lsoa"
DATASET_LOCATION = None  # Exemplo: "EU" ou "US" se necessário

# ---------------------
# CAMINHOS DE SAÍDA
# ---------------------

OUTPUT_DIR = r"C:\Users\rodri\OneDrive\Documents\programacao\output"

# Arquivos de saída
CSV_VALIDATION = os.path.join(OUTPUT_DIR, "crime_by_lsoa_validation.csv")
CSV_AGG = os.path.join(OUTPUT_DIR, "crime_by_lsoa_agg.csv")
PARQUET_TS = os.path.join(OUTPUT_DIR, "crime_by_lsoa_ts.parquet")

# Fallback local (arquivo local caso o BigQuery falhe)
LOCAL_CSV_FALLBACK = os.path.join(OUTPUT_DIR, "crime_by_lsoa_fallback_input.csv")

# ---------------------
# CONFIGURAÇÃO DE LOGS
# ---------------------

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------
# FUNÇÃO PARA CONECTAR AO BIGQUERY
# ---------------------

def create_bq_client(project_id: str) -> bigquery.Client:
    """Cria uma conexão com o BigQuery usando o projeto especificado."""
    try:
        client = bigquery.Client(project=project_id)
        logger.info(
            "BigQuery client criado - projeto: %s | cred_type: %s",
            client.project,
            type(client._credentials)
        )
        return client
    except Exception as e:
        logger.exception("Erro ao criar o cliente BigQuery: %s", e)
        raise

# ---------------------
# FUNÇÃO EXECUTAR QUERY E RETORNAR DATAFRAME
# ---------------------

def run_query_to_df(client: bigquery.Client, sql: str, location: str = None) -> pd.DataFrame:
    """Executa uma query SQL no BigQuery e retorna o resultado como um DataFrame do Pandas."""
    if location:
        job = client.query(sql, location=location)
    else:
        job = client.query(sql)

    df = job.result().to_dataframe()
    return df

# ---------------------
# FUNÇÃO PRINCIPAL
# ---------------------

def main():
    """Controla o fluxo da aplicação: Consultas, tratamento e salvamento dos dados."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    start_time = datetime.now(timezone.utc)
    logger.info("Início do processo - %s", start_time.isoformat())

    client = None
    df_preview = None
    df_validation = None
    df_agg = None
    df_ts = None

    try:
        # 1. Conexão ao BigQuery
        client = create_bq_client(PROJECT_ID)

        # 2. Query 1 - Preview dos Dados
        sql_preview = f"""
        SELECT *
        FROM `{TABLE_ID}`
        LIMIT 10
        """
        df_preview = run_query_to_df(client, sql_preview, DATASET_LOCATION)
        logger.info("Preview dos dados carregado com sucesso. Linhas: %d", len(df_preview))
        print("\n--- PREVIEW DOS DADOS ---")
        print(df_preview.head())

        # 3. Query 2 - Validação e Auditoria de Qualidade dos Dados
        sql_validation = f"""
        SELECT
            COUNT(*) AS total_linhas,
            
            -- Checagem de Valores Nulos (Completude)
            COUNTIF(lsoa_code IS NULL) AS null_lsoa_code,
            COUNTIF(borough IS NULL) AS null_borough,
            COUNTIF(major_category IS NULL) AS null_major_category,
            COUNTIF(minor_category IS NULL) AS null_minor_category,
            COUNTIF(value IS NULL) AS null_value,
            COUNTIF(year IS NULL) AS null_year,
            COUNTIF(month IS NULL) AS null_month,
            
            -- Checagem de Inconsistências Lógicas
            COUNTIF(value < 0) AS valores_negativos,
            COUNTIF(value = 0) AS registros_zerados,
            COUNTIF(month < 1 OR month > 12) AS meses_invalidos,
            
            -- Amplitude Temporal e Cardinalidade de Categorias
            MIN(year) AS ano_minimo,
            MAX(year) AS ano_maximo,
            COUNT(DISTINCT borough) AS qtd_boroughs_unicos,
            COUNT(DISTINCT major_category) AS qtd_categorias_principais
        FROM `{TABLE_ID}`
        """
        
        df_val_raw = run_query_to_df(client, sql_validation, DATASET_LOCATION)
        
        # Transpõe o resultado para facilitar a leitura no console e no CSV
        df_validation = df_val_raw.T.reset_index()
        df_validation.columns = ["metrica_validacao", "valor"]
        
        logger.info("Validação de dados concluída.")
        print("\n--- RELATÓRIO DE VALIDAÇÃO E QUALIDADE DOS DADOS ---")
        print(df_validation.to_string(index=False))

        # 4. Query 3 - Dados Agregados por Categoria (Com filtro de consistência)
        sql_agg = f"""
        SELECT major_category, SUM(value) AS total_crimes
        FROM `{TABLE_ID}`
        WHERE value >= 0  -- Garante que não usaremos registros inconsistentes se existirem
        GROUP BY major_category
        ORDER BY total_crimes DESC
        """
        df_agg = run_query_to_df(client, sql_agg, DATASET_LOCATION)
        logger.info("Dados agregados processados. Linhas: %d", len(df_agg))

        # 5. Query 4 - Série Temporal por Ano/Mês
        sql_ts = f"""
        SELECT year, month, SUM(value) AS total_crimes
        FROM `{TABLE_ID}`
        WHERE value >= 0 AND month BETWEEN 1 AND 12
        GROUP BY year, month
        ORDER BY year, month
        """
        df_ts = run_query_to_df(client, sql_ts, DATASET_LOCATION)
        logger.info("Série temporal processada. Linhas: %d", len(df_ts))

    except Exception as bq_err:
        # Fallback para execução offline/local
        logger.warning("Erro ao executar consultas no BigQuery: %s", bq_err)

        if os.path.exists(LOCAL_CSV_FALLBACK):
            logger.info("Usando CSV local como alternativa de fallback.")
            df_fallback = pd.read_csv(LOCAL_CSV_FALLBACK)
            df_fallback.to_csv(
                os.path.join(OUTPUT_DIR, "crime_by_lsoa_fallback.csv"), index=False
            )
        else:
            logger.error("CSV local de fallback não encontrado. Encerrando processo.")
            raise

    # ---------------------
    # SALVAMENTO DOS RESULTADOS
    # ---------------------
    
    if df_validation is not None and not df_validation.empty:
        df_validation.to_csv(CSV_VALIDATION, index=False)
        logger.info("Relatório de validação salvo em: %s", CSV_VALIDATION)

    if df_agg is not None and not df_agg.empty:
        df_agg.to_csv(CSV_AGG, index=False)
        logger.info("Arquivo CSV de agregação salvo em: %s", CSV_AGG)

    if df_ts is not None and not df_ts.empty:
        try:
            df_ts.to_parquet(PARQUET_TS, index=False)
            logger.info("Arquivo Parquet de Série Temporal salvo em: %s", PARQUET_TS)
        except Exception as e:
            logger.warning("Falha ao salvar Parquet (%s). Salvando como CSV em alternativa.", e)
            csv_alternative = PARQUET_TS.replace(".parquet", ".csv")
            df_ts.to_csv(csv_alternative, index=False)
            logger.info("Arquivo CSV alternativo salvo em: %s", csv_alternative)

    # ---------------------
    # FINALIZAÇÃO
    # ---------------------

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    logger.info("Processo finalizado - Duração total: %.1f segundos", duration)


if __name__ == "__main__":
    main()