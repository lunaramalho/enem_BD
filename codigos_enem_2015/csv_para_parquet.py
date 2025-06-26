import pandas as pd
import os

csv_path = "C:/Users/lunas/Downloads/enem2015/MICRODADOS_ENEM_2015.csv"
output_parquet_dir = "parquet_chunks"
output_csv_path = "MICRODADOS_ENEM_2015_saida.csv"
output_parquet_path = "MICRODADOS_ENEM_2015_saida.parquet"

usar_chunks = True
salvar_csv = False
usar_parquet = True
chunksize = 50000
usar_amostra = False
amostra_linhas = 50000
csv_sep = ';'
csv_encoding = 'latin1'

if usar_amostra:
    print(f"Lendo as primeiras {amostra_linhas} linhas do CSV...")
    df = pd.read_csv(csv_path, sep=csv_sep, encoding=csv_encoding, nrows=amostra_linhas)
    if usar_parquet:
        amostra_parquet_path = os.path.join(output_parquet_dir, output_parquet_path)
        os.makedirs(output_parquet_dir, exist_ok=True)
        df.to_parquet(amostra_parquet_path, index=False)
        print(f"Arquivo Parquet da amostra salvo em: {amostra_parquet_path}")
    if salvar_csv:
        amostra_csv_path = output_csv_path.replace(".csv", "_amostra.csv")
        df.to_csv(amostra_csv_path, sep=csv_sep, index=False, encoding=csv_encoding)
        print(f"Arquivo CSV da amostra salvo em: {amostra_csv_path}")

elif usar_chunks:
    print(f"Convertendo CSV inteiro para vários arquivos Parquet em chunks de {chunksize} linhas...")

    os.makedirs(output_parquet_dir, exist_ok=True)

    leitor_chunks = pd.read_csv(csv_path, sep=csv_sep, encoding=csv_encoding, chunksize=chunksize)
    numero_chunk = 1

    for chunk in leitor_chunks:
        parquet_path = os.path.join(output_parquet_dir, f"MICRODADOS_ENEM_2015_chunk_{numero_chunk}.parquet")
        chunk.to_parquet(parquet_path, index=False)
        print(f"Chunk {numero_chunk} salvo em {parquet_path}")
        numero_chunk += 1

    print("Conversão em chunks concluída!")

    if salvar_csv:
        print("Salvar CSV completo com chunks pode ser custoso e não implementado aqui.")

else:
    print("Lendo o CSV inteiro para conversão...")
    df = pd.read_csv(csv_path, sep=csv_sep, encoding=csv_encoding)
    if usar_parquet:
        parquet_path = os.path.join(output_parquet_dir, output_parquet_path)
        os.makedirs(output_parquet_dir, exist_ok=True)
        df.to_parquet(parquet_path, index=False)
        print(f"Arquivo Parquet salvo em: {parquet_path}")
    if salvar_csv:
        df.to_csv(output_csv_path, sep=csv_sep, index=False, encoding=csv_encoding)
        print(f"Arquivo CSV salvo em: {output_csv_path}")
