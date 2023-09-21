import requests
import pandas as pd
import os

class ListaDeRepositorios():
        
    def __init__(self, usuario, token):
        self._usuario = usuario
        self._token = token
       
    def requisicao_api_github(self, endpoint):
        authorization = f'token {self._token}' 
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": authorization,
        } 
        
        resposta = requests.get(
            endpoint, headers=headers)
        if resposta.status_code == 200:
            return resposta.json()
        else:
            return resposta.status_code

    def buscar_issues(self, repositorio):
        endpoint = f'https://api.github.com/repos/{self._usuario}/{repositorio}/issues'
        return self.requisicao_api_github(endpoint)

    def buscar_milestones(self, repositorio):
        endpoint = f'https://api.github.com/repos/{self._usuario}/{repositorio}/milestones'
        return self.requisicao_api_github(endpoint)

    def buscar_labels(self, repositorio):
        endpoint = f'https://api.github.com/repos/{self._usuario}/{repositorio}/labels'
        return self.requisicao_api_github(endpoint)

    def converter_para_parquet(self, data, arquivo_parquet):
        if os.path.exists(arquivo_parquet):
            os.remove(arquivo_parquet)

        pd.DataFrame(data).to_parquet(arquivo_parquet)

usuario = "usuario"
token = "token"
lista_repositorios = ListaDeRepositorios(usuario, token)

repositorio = "nome_do_repositorio"
issues = lista_repositorios.buscar_issues(repositorio)
milestones = lista_repositorios.buscar_milestones(repositorio)
labels = lista_repositorios.buscar_labels(repositorio)

arquivo_parquet_issues = "caminho_do_arquivo.parquet"
arquivo_parquet_milestones = "caminho_do_arquivo.parquet"
arquivo_parquet_labels = "caminho_do_arquivo.parquet"

lista_repositorios.converter_para_parquet(issues, arquivo_parquet_issues)
lista_repositorios.converter_para_parquet(milestones, arquivo_parquet_milestones)
lista_repositorios.converter_para_parquet(labels, arquivo_parquet_labels)

print(f'Dados de issues salvos em {arquivo_parquet_issues}')
print(f'Dados de milestones salvos em {arquivo_parquet_milestones}')
print(f'Dados de labels salvos em {arquivo_parquet_labels}')
