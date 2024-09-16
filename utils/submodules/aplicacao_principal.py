# importação de bibliotecas
from utils.utils import funcoes
from pathlib import Path
import pandas as pd

class DinDinDireito:
    def __init__(self, args):
        self.tipo_arq = ['.xlsx', '.xls', '.csv']
        self.path_arq = funcoes.val_exist(args['PATH_ARCHIVE'], self.tipo_arq)
        self.path_Ymal=   funcoes.val_exist(args['PATH_YAML'],['.yaml'])
        self.ext_sor = Path(self.path_arq).suffix
        self.path_sor =  args['CAMINHO_BUCKET'] + '\\' + '02_SOR'
        self.path_sot =  args['CAMINHO_BUCKET'] + '\\' + '03_SOT'
        self.yaml = funcoes.importa_yaml(self.path_Ymal)
        self.xls = funcoes.importa_xls(self.path_arq)
        self.tipo = funcoes.valida_deb_cred(self.xls, self.yaml)
        self.campo_ref = self.yaml[self.tipo]['data_ref']
        self.raw_df = funcoes.importa_raw_df(self.path_arq, 
                                             self.yaml[self.tipo]['aba'])
        self.aba = self.yaml[self.tipo]['aba']
        self.colunas = [item['campo'] for item in self.yaml[self.tipo]['colunas']]
        self.schema = self.yaml[self.tipo]['colunas']
        self.delimitador = self.yaml[self.tipo]['delimitador_inferior']
        self.pd_clean_df = self.estrutura_dados()

    def limites_verticais(self, df, tipo):
        '''
        Função responsável identificar e limitar o início e o fim dos dados
        verticalmente.
        entrada: 
            df(df) - DataFrame não tratado \n
            tipo(str) - variável que informa se o código irá limitar inferior
                        ou superiormente a base. ('superior' ou 'inferior')
        saida: 
            dfsaida(df) - dataframe tratado.
        '''
        # identifica o tipo de limitador 
        if      tipo == 'superior':
            limitador = self.colunas[0]
        elif    tipo == 'inferior':
            limitador = self.delimitador
        # Buscando o primeiro valor em todo o DataFrame
        linha_encontrada = None
        for coluna in df.columns:
            linha_temp = df.index[df[coluna] == limitador].tolist()
            if linha_temp:
                linha_encontrada = linha_temp[0]
                break
        # Verificando se o valor foi encontrado e fazendo o corte no DataFrame
        if linha_encontrada is not None and tipo=='superior':
            novo_cabecalho = df.iloc[linha_encontrada]
            dfsaida = df.iloc[linha_encontrada+1:]
            dfsaida.columns = novo_cabecalho
        elif linha_encontrada is not None and tipo=='inferior':
            dfsaida = df.iloc[:linha_encontrada]
        else:
            dfsaida = df
        dfsaida = dfsaida.reset_index(drop=True)
        return dfsaida
    
    def limites_horizontais(self, df):
        '''
        Função responsável identificar e limitar o início e o fim dos dados
        horizontalmente.
        entrada: 
            df(df) - DataFrame não tratado
        saida: 
            dfsaida(df) - dataframe tratado.
        '''
        # identifica as colunas de início e fim.
        coluna_inicial = self.colunas[0]
        coluna_final = self.colunas[-1]
        # Buscando o indice cada coluna
        for coluna in df.columns:
            if coluna == coluna_inicial:
                indice_inicial = df.columns.get_loc(coluna)
        for coluna in df.columns:
            if coluna == coluna_final:
                indice_final = df.columns.get_loc(coluna)
        # Corte no DataFrame
        dfsaida = df.iloc[:, indice_inicial:indice_final+1]
        dfsaida = dfsaida.reset_index(drop=True)
        return dfsaida
    
    def ajusta_colunas(self, df):
        '''
        Função responsável por ajustar eventuais colunas em branco entre as
        colunas tratadas.
        entrada: df(df) - DataFrame com as colunas definidas pelos usuários
        saida: dfsaida(df) - colunas em banco removidas.
        '''
        # Identificar colunas com títulos vazios (geralmente nomeadas como 
        # 'Unnamed: x')
        if (not df.filter(like='Unnamed').empty or 
            len(df.columns[pd.isna(df.columns)].tolist())>0):
            colunas_a_remover = [coluna for coluna in df.columns 
                                if 'Unnamed' in str(coluna) or pd.isna(coluna)]
            # Remover estas colunas do DataFrame
            dfsaida = df.drop(columns=colunas_a_remover).copy()
        else:
            dfsaida = df.copy()
        dfsaida = dfsaida[self.colunas].reset_index(drop=True)
        return dfsaida
        
    def estrutura_dados(self):      
        clean_df1 = self.limites_verticais(self.raw_df,'superior')
        clean_df2 = self.limites_verticais(clean_df1,'inferior')
        self.colunas = funcoes.identifica_ordem(self.colunas, clean_df2)
        clean_df3 = self.limites_horizontais(clean_df2)
        clean_df4 = self.ajusta_colunas(clean_df3)
        return clean_df4