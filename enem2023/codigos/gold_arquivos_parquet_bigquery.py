import os
from google.cloud import bigquery, storage
from google.oauth2 import service_account



# FUNÇÃO PRINCIPAL
def carregar_pastas_para_bigquery(bucket_name, base_path, dataset_id):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=base_path))

    pastas = set()
    for blob in blobs:
        partes = blob.name.split('/')
        if len(partes) >= 3 and partes[-1].endswith('.parquet'):
            pastas.add(partes[2]) 

    for pasta in sorted(pastas):
        print(f"\nCarregando arquivos da pasta '{pasta}'...")
        arquivos_parquet = []

        for blob in blobs:
            if f"{base_path}/{pasta}/" in blob.name and blob.name.endswith('.parquet'):
                uri = f"gs://{bucket_name}/{blob.name}"
                arquivos_parquet.append(uri)
                print(f"  ➤ Encontrado: {uri}")

        if not arquivos_parquet:
            print(f"⚠ Nenhum arquivo Parquet encontrado para a pasta '{pasta}'.")
            continue

        # Define a referência da tabela
        table_ref = bigquery_client.dataset(dataset_id).table(pasta)

        # Configuração da tarefa de carga
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
        )

        # Envia os dados para o BigQuery
        print(f"Iniciando carga de {len(arquivos_parquet)} arquivos para a tabela '{pasta}'...")
        load_job = bigquery_client.load_table_from_uri(
            arquivos_parquet, table_ref, job_config=job_config
        )

        load_job.result()
        print(f"Tabela '{pasta}' carregada com sucesso com {len(arquivos_parquet)} arquivos.")

# EXECUTAR
if __name__ == "__main__":

    # CONFIGURAÇÕES
    project_id = 'analauramagalhaes-lunaramalho'
    dataset_id = 'dados_enem_2023'
    bucket_name = 'enemm-bucket'
    parquet_base_path = 'silver/parquet'
    credentials_path = 'C:/Users/lunas/Downloads/EngenhariaDadosEnem-main (1)/chave/analauramagalhaes-lunaramalho-43207bea500d.json'

    # AUTENTICAÇÃO
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    bigquery_client = bigquery.Client(project=project_id, credentials=credentials)
    storage_client = storage.Client(project=project_id, credentials=credentials)

    carregar_pastas_para_bigquery(bucket_name, parquet_base_path, dataset_id)
