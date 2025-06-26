import os
import tempfile
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

# FUNÇÃO: Corrige colunas com tipo FLOAT64 que devem ser INT64
def corrigir_tipos_parquet(blob, temp_dir, storage_client):
    # Baixar arquivo Parquet original
    local_path = os.path.join(temp_dir, os.path.basename(blob.name))
    blob.download_to_filename(local_path)

    # Ler Parquet como DataFrame
    df = pd.read_parquet(local_path)

    # Colunas que precisam ser convertidas para INTEGER (nullable com 'Int64')
    colunas_para_converter = {
        'Q005': 'Int64',
        'TP_PRESENCA_CH': 'Int64',
        'TP_PRESENCA_CN': 'Int64',
        'TP_PRESENCA_LC': 'Int64',
        'TP_PRESENCA_MT': 'Int64',
        'TP_LINGUA': 'Int64',
        'TP_STATUS_REDACAO': 'Int64',
        'TP_NOTA_REDACAO': 'Int64',
        # Adicione aqui mais colunas, se novos erros aparecerem
    }

    for coluna, tipo in colunas_para_converter.items():
        if coluna in df.columns:
            print(f"    ➤ Convertendo coluna '{coluna}' para {tipo}")
            try:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype(tipo)
            except Exception as e:
                print(f"    ⚠ Erro ao converter coluna {coluna}: {e}")

    # Salvar o arquivo corrigido
    df.to_parquet(local_path, index=False)
    return local_path

# FUNÇÃO PRINCIPAL: Corrige e carrega para o BigQuery
def carregar_pastas_para_bigquery(bucket_name, base_path, dataset_id):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=base_path))

    pastas = set()
    for blob in blobs:
        partes = blob.name.split('/')
        if len(partes) >= 3 and partes[-1].endswith('.parquet'):
            pastas.add(partes[2])  # Nome da pasta (ex: 'prova')

    for pasta in sorted(pastas):
        print(f"\n📂 Carregando arquivos da pasta '{pasta}'...")
        arquivos_parquet = []

        with tempfile.TemporaryDirectory() as temp_dir:
            for blob in blobs:
                if f"{base_path}/{pasta}/" in blob.name and blob.name.endswith('.parquet'):
                    print(f"  ➤ Corrigindo tipos no arquivo: {blob.name}")
                    arquivo_corrigido = corrigir_tipos_parquet(blob, temp_dir, storage_client)

                    # Upload para GCS (temporário)
                    temp_blob_name = f"temp/{pasta}/{os.path.basename(arquivo_corrigido)}"
                    temp_blob = bucket.blob(temp_blob_name)
                    temp_blob.upload_from_filename(arquivo_corrigido)

                    uri = f"gs://{bucket_name}/{temp_blob_name}"
                    arquivos_parquet.append(uri)

        if not arquivos_parquet:
            print(f"⚠ Nenhum arquivo Parquet corrigido encontrado para a pasta '{pasta}'.")
            continue

        # Referência da tabela
        table_ref = bigquery_client.dataset(dataset_id).table(pasta)

        # Configuração da carga
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Carregar no BigQuery
        print(f"🚀 Iniciando carga de {len(arquivos_parquet)} arquivos para a tabela '{pasta}'...")
        load_job = bigquery_client.load_table_from_uri(arquivos_parquet, table_ref, job_config=job_config)
        load_job.result()
        print(f"✅ Tabela '{pasta}' carregada com sucesso.")

# EXECUÇÃO
if __name__ == "__main__":

    # CONFIGURAÇÕES
    project_id = 'analauramagalhaes-lunaramalho'
    dataset_id = 'dados_enem_2015'
    bucket_name = 'enemm2015_bucket'
    parquet_base_path = 'silver/parquet'
    credentials_path = 'C:/Users/lunas/Downloads/enem2015/chave/analauramagalhaes-lunaramalho-43207bea500d.json'

    # AUTENTICAÇÃO
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    storage_client = storage.Client(project=project_id, credentials=credentials)

    carregar_pastas_para_bigquery(bucket_name, parquet_base_path, dataset_id)
