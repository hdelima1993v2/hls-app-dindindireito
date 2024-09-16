import pandas as pd
import traceback
import shutil
import yaml
import os

class funcoes():
    def cortar_texto(texto, ini_char, end_char):
        '''
        Função responsável por contar um determinado texto por uma palavra 
        inicial e final
        '''
        inicio = texto.find(ini_char)
        if inicio == -1:
            return ""
        fim = texto.find(end_char, inicio)
        if fim == -1:
            return texto[inicio:]
        return texto[inicio:fim]

    def valida_deb_cred(excelfile, dicio_yaml):
        '''
        Valida se o arquivo existente no diretório é de natureza 'crédito'
        ou 'débito' a partir da tag identificador

        entrada: 
            excelfile(xls): arquivo excel completo com todas as abas
            dicio_yaml(dict): dicionario parametros.yaml
        saida:
            categoria(str): 'credito' ou 'debito'
        '''
        try:
            dicio_xls = {}
            saida = False
            for categoria, detalhes in dicio_yaml.items():
                identificador = detalhes.get('identificador', None)
                if identificador is not None and identificador != '':
                    for aba in excelfile.sheet_names:
                        dicio_xls[aba] = pd.read_excel(excelfile, 
                                                       sheet_name=aba)
                        # Iterar por todas as linhas e colunas do DataFrame
                        for coluna in dicio_xls[aba].columns:
                            for valor in dicio_xls[aba][coluna]:
                                if identificador in str(valor):
                                    saida = True
                                    break
                            if saida:
                                break
                        if saida:
                                break
                else:
                    print('ALERTA: Identificador não incluído para categoria '
                        f'{categoria}')
            #retorna a categoria caso ela seja encontrado e None caso contrário
            return categoria if saida else None
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: valida_deb_cred\n'
                  f'linha onde o erro ocorreu:{erro}')
            
    def val_exist(strpathdir, tipo_extensao):
        '''
        verifica se existe um único arquivo no diretório que tenha extensão 
        contida na variavel tipo_extensao, onde tipo_extensao é uma 
        lista de extensões possíveis
        '''
        try:
            arquivos = os.listdir(strpathdir)
            arquivos = [arquivo for arquivo in arquivos if 
                        arquivo.endswith(tuple(tipo_extensao))]
            if len(arquivos) == 1:
                return os.path.join(strpathdir, arquivos[0])
            else:
                print('ERRO - Não há nenhum arquivo com extensão(s) '+ 
                      ', '.join(tipo_extensao) + 
                      ' no diretório ou há mais de um arquivo com a mesma '
                      'extensão\n'
                      f'local: {strpathdir}')
                exit()
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: val_exist\n'
                  f'linha onde o erro ocorreu:{erro}\n'
                  'Arquivo não está no diretório ou não é um arquivo excel\n'
                  f'local: {strpathdir}')
                     
    
    def importa_yaml(strpathYmal):
        '''
        Função responsável por importar um arquivo yaml e retornar um objeto
        do tipo dict
        '''
        try:
            with open(strpathYmal, 'r', encoding='utf-8') as arquivo:
                return yaml.safe_load(arquivo)
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: importa_yaml \n'
                  f'linha onde o erro ocorreu:{erro} \n'
                  'Arquivo não está no diretório ou não é um arquivo yaml'
                  'ou não está no formato esperado de um yaml\n'
                  f'Arquivo: {strpathYmal}')
    
    def importa_xls(strpathArq):
        '''
        Função responsável por importar um arquivo excel e retornar um objeto
        do tipo ExcelFile
        '''
        try:
            return pd.ExcelFile(strpathArq)
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: importa_xls\n'
                  f'linha onde o erro ocorreu:{erro}\n'
                  'Arquivo não está no diretório ou não é um arquivo excel\n')
            
    def importa_raw_df(strpathArq, aba):
        '''
        Função responsável por importar um arquivo excel e retornar um dataframe
        com os dados da aba especificada
        '''
        try:
            return pd.read_excel(strpathArq, engine='xlrd', sheet_name=aba)
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: importa_raw_df'
                  f'linha onde o erro ocorreu:{erro}'
                  'Arquivo não está no diretório ou não é um arquivo excel')
        
    def move_SoR(args):
        '''
        Função responsável por mover o arquivo processado para a pasta de
        processados
        '''
        try:
            path_arq = args['path_arq']
            path_sor = (args['path_sor'] + '\\SOR_' + 
                        args['tipo'] + '_' + 
                        args['data_ref'] +args['ext'] )
    
            shutil.move(path_arq, path_sor)
        except Exception as e:
            erro = traceback.format_exc()
            erro = funcoes.cortar_texto(erro,'line',',')
            print(f'ERRO - Mensagem de erro: {e}\n'
                   'Arquivo: ../src/utils/utils.py \n'
                   'Função: move_SoR\n'
                  f'linha onde o erro ocorreu:{erro}\n'
                  'Arquivo não está no diretório ou não é um arquivo excel\n')

    def coleta_argumentos(paramentros):
        '''
        Função responsável por coletar os argumentos passados por linha de
        comando e retornar um dicionário com os argumentos passados
        '''
        args_dict = {}
        argv = paramentros[1:]
        for i in range(0, len(argv), 2):
            if argv[i].startswith('--'):
                key = argv[i][2:]
                if i+1 < len(argv) and not argv[i+1].startswith('--'):
                    value = argv[i+1]
                    args_dict[key] = value
        return args_dict

    def identifica_ordem(lista_campos, df):
        '''
        Função responsável por identificar a ordem dos campos em um dataframe
        '''
        return [col for col in df.columns if col in lista_campos]
    
    
        

        

    



