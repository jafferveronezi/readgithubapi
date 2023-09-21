import pandas as pd

caminho_arquivo_parquet = "caminho_do_arquivo.parquet"

df = pd.read_parquet(caminho_arquivo_parquet)

print(df)