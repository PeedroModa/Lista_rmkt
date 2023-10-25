import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import os

diretorio = r'C:\Users\Pedro Moda\Desktop\clientes'


def on_file_created(event):
    arquivo_novo = event.src_path
    print(f"Novo arquivo adicionado: {arquivo_novo}")

    dfs = []

    for arquivo in os.listdir(diretorio):
        if arquivo.endswith('.xlsx'):
            caminho_arquivo = os.path.join(diretorio, arquivo)

            df = pd.read_excel(caminho_arquivo)
            dfs.append(df)

    dados = pd.concat(dfs, ignore_index=False)

    # Excluindo colunas que não irei utlizar.
    colunas_exclusao = ['Núm', 'Feito em']
    dados = dados.drop(colunas_exclusao, axis=1)

    # Alterando a coluna para o tipo string.
    dados['Nome'] = dados['Nome'].astype(str)
    # Criando uma nova coluna apenas com o primeiro nome.
    dados['fn'] = dados['Nome'].apply(lambda x: x.split()[0])
    # Criando uma nova coluna apenas com o último nome.
    dados['ln'] = dados['Nome'].apply(lambda x: x.split()[-1])
    # Excluindo a coluna Nome.
    dados = dados.drop(['Nome'], axis=1)
    # Renomeando a coluna Primeiro nome para Nome.
    filtro_nomes = {'E-mail': 'email', 'Cidade': 'ct',
                    'Estado': 'st', 'País': 'country', 'CEP': 'zip'}
    dados = dados.rename(columns=filtro_nomes)
    # Ordenando as colunas para ficar de fácil visualização.
    dados = dados[['email', 'fn', 'ln', 'zip', 'ct', 'st', 'country']]
    # Coletando apenas aquelas linhas onde os valores de nome, são diferentes do e-mail.
    dados = dados[dados['fn'] != dados['email']]

    # Filtrando apenas os valores de Nome e Sobrenome acima de 3 caracteres.
    dados = dados[dados['fn'].str.len() > 3]
    # Verificando se existem valores numéricos nas colunas nome e sobrenome.
    dados = dados[dados['ln'].str.len() > 3]

    linhas1 = dados[dados['ln'].str.isnumeric()]
    linhas2 = dados[dados['fn'].str.isnumeric()]
    # Rodando uma função que irá trocar os valores numéricos (quando estão na coluna Sobrenome) pelo nome da cidade. Ex: Pedro de Barra Bonita

    def subs_sobre(row):
        if row['ln'].isnumeric():
            return 'de' + row['ct']
        else:
            return row['ln']

    dados['ln'] = dados.apply(subs_sobre, axis=1)
    dados = dados[~dados['fn'].str.isnumeric()]

    # Conte o número de linhas do DataFrame
    numero_de_linhas = len(dados)

    # Exiba o número de linhas
    print(f"Número de linhas do arquivo: {numero_de_linhas}")
    dados.to_csv('Lista_atualizada.csv', index=False, encoding='utf-8-sig')


# Crie um observador (watcher) e adicione um manipulador de eventos
observer = Observer()
event_handler = FileSystemEventHandler()
event_handler.on_created = on_file_created
observer.schedule(event_handler, path=diretorio, recursive=False)

# Inicie o observador
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()