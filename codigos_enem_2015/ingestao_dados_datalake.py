from google.cloud import storage

client = storage.Client.from_service_account_json('C:/Users/lunas/Downloads/enem2015/chave/analauramagalhaes-lunaramalho-43207bea500d.json')

bucket_name = 'enemm2015_bucket'
file_path = 'C:/Users/lunas/Downloads/enem2015/MICRODADOS_ENEM_2015_saida_amostra.csv'
destination_blob_name = 'bronze/microdados_enem.csv' 

bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(file_path)

print(f"Arquivo enviado para gs://{bucket_name}/{destination_blob_name}")
