import os
import pandas as pd
import gcsfs

def processar_dados_gcs(bucket_name, chave_json, formato_entrada, caminho_particionado,
                        limpar_colunas, pasta_saida_local, grupos, particionado=False):
    # Autentica no GCS
    fs = gcsfs.GCSFileSystem(token=chave_json)

    # Lista arquivos no bucket
    if particionado:
        arquivos = fs.glob(f'{bucket_name}/{caminho_particionado}*.{formato_entrada}')
    else:
        arquivos = [f'{bucket_name}/{caminho_particionado}.{formato_entrada}']

    for caminho_completo in arquivos:
        print(f'Processando {caminho_completo}...')
        nome_base = os.path.splitext(os.path.basename(caminho_completo))[0]

        # Lê o Parquet diretamente do GCS
        with fs.open(caminho_completo, 'rb') as f:
            df = pd.read_parquet(f)

        # Remove colunas que o usuário quer limpar
        for col in limpar_colunas:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        # Para cada grupo de colunas, cria um arquivo separado
        for grupo, colunas in grupos.items():
            colunas_existentes = [col for col in colunas if col in df.columns]
            if not colunas_existentes:
                continue

            df_subset = df[colunas_existentes]

            caminho_saida = f"{bucket_name}/{pasta_saida_local}/{grupo}/{nome_base}_{grupo}.parquet"
            print(f'Salvando arquivo {caminho_saida}...')

            # Salva diretamente no GCS
            with fs.open(caminho_saida, 'wb') as f_out:
                df_subset.to_parquet(f_out, index=False)

    print("Processamento finalizado.")

if __name__ == "__main__":
    # Configurações
    bucket_name = 'enemm-bucket'
    chave_json = 'C:/Users/lunas/Downloads/EngenhariaDadosEnem-main (1)/chave/analauramagalhaes-lunaramalho-43207bea500d.json'

    # Parâmetros para dados particionados
    formato_entrada = 'parquet'
    caminho_particionado = 'bronze/parquet/MICRODADOS_ENEM_2023_chunk_'
    
    #******************Transformaçoes*********************************
    # Adicione suas transformacoes aqui
    remover_colunas = ['Q023', 'Q020', 'Q019', 'Q018', 'Q017', 'Q013', 'Q015', 'Q014', 'Q016', 'Q021', 'NU_NOTA_COMP1', 'NU_NOTA_COMP2', 'NU_NOTA_COMP3', 'NU_NOTA_COMP4', 'NU_NOTA_COMP5',
                       'CO_PROVA_CN', 'CO_PROVA_CH', 'CO_PROVA_LC', 'CO_PROVA_MT', 'TX_RESPOSTAS_CN', 'TX_RESPOSTAS_CH', 'TX_RESPOSTAS_MT', 'TX_RESPOSTAS_LC', 'TX_GABARITO_CN',
                       'TX_GABARITO_CH', 'TX_GABARITO_MT', 'TX_GABARITO_LC', 'CO_UF_ESC', 'SG_UF_ESC', 'TP_SIT_FUNC_ESC', 'TP_ENSINO', 'NU_ANO'
                       
                       
                       ]

    # Definição dos grupos de dados
    # Exemplos possíveis
    grupos_dados = {
        "participante": [
            "NU_INCRICAO", "TP_FAIXA_ETARIA", "TP_SEXO", "TP_ESTADO_CIVIL", "TP_COR_RACA", "TP_NACIONALIDADE", "TP_ST_CONCLUSAO", "TP_ANO_CONCLUIU", "TP_ESCOLA", "IN_TREINEIRO"
        ],
        "escola": [
            "NU_INSCRICAO", "CO_MUNICIPIO_ESC", "NO_MUNICIPIO_ESC", "TP_DEPENDENCIA_ADM_ESC", "TP_LOCALIZACAO_ESC", "CO_MUNICIPIO_PROVA", "NO_MUNICIPIO_PROVA"
        ],
        "prova": [
            "NU_INCRICAO", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_MT", "NU_NOTA_LC", "TP_PRESENCA_CH", "TP_PRESENCA_CN", "TP_PRESENCA_MT", "TP_PRESENCA_LC", "TP_STATUS_REDACAO", "NU_NOTA_REDACAO"
        ],
        "perguntas_sociais": [
            "NU_INCRICAO", "Q001", "Q002", "Q003", "Q004", "Q005", "Q006", "Q007", "Q008", "Q009", "Q010", "Q011", "Q012", "Q022", "Q024", "Q025"
        ],
        
    }

    # Executa o processamento
    processar_dados_gcs(
        bucket_name=bucket_name,
        chave_json=chave_json,
        formato_entrada=formato_entrada,
        caminho_particionado=caminho_particionado,
        limpar_colunas=remover_colunas,
        pasta_saida_local='silver/parquet',
        grupos=grupos_dados,
        particionado=True
    )
