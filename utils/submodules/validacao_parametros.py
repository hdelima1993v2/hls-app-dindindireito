from utils.utils import funcoes

class validador_yaml():
    def __init__(self, yaml_path):
        self.path_Ymal= funcoes.val_exist(yaml_path,['.yaml'])
        self.yaml_data = funcoes.importa_yaml(self.path_Ymal) 
        self.erros = []
        self.parametros_obrigatorios = ['aba', 'colunas', 'data_ref', 
            'delimitador_inferior', 'identificador', 'campo', 'nome_final',
            'tipo', 'aceita_nulos','formato']
        self.parametros_preenchidos = ['aba', 'colunas', 'campo', 'nome_final',
            'tipo', 'aceita_nulos', 'data_ref','formato']
        self.valida_tipo = ['data', 'numero', 'texto']
        self.valida_aceita_nulos = ['s', 'n']
        self.valida_nome_final = ['lancamento', 'data', 'valor']
        self.valida_formato = ['%d/%m/%Y', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S']

#==============================================================================
# Funções auxiliares
#==============================================================================
    def extrair_todas_as_chaves(self,dicionario):
        '''
        Extrai todas as chaves presentes em um dicionário, incluindo chaves de
        sub-dicionários e listas de dicionários.
        '''
        chaves = []
        for chave, valor in dicionario.items():
            chaves.append(chave)
            if isinstance(valor, dict):
                chaves.extend(self.extrair_todas_as_chaves(valor))
            elif isinstance(valor, list):
                for item in valor:
                    if isinstance(item, dict):
                        chaves.extend(self.extrair_todas_as_chaves(item))
        return chaves
    
    def encontrar_parametros_com_lista(self, dicionario):
        '''
        Função responsável por encontrar parâmetros que possuem listas de
        dicionários como valor, e retornar um dicionário com esses parâmetros
        e suas respectivas listas.
        '''
        parametros_com_lista = {}
        def processar_elemento(elemento, chave_anterior=''):
            if isinstance(elemento, dict):
                for chave, valor in elemento.items():
                    nova_chave = chave
                    processar_elemento(valor, nova_chave)
            elif isinstance(elemento, list):
                parametros_com_lista[chave_anterior] = elemento
        processar_elemento(dicionario)
        return parametros_com_lista

    def normalizar_dicionario(self, dicionario):
        '''
        Função responsável por normalizar um dicionário, transformando todos os
        valores em strings e removendo sub-dicionários e listas de dicionários.
        '''
        normalizado = {}
        def processar_elemento(elemento, prefixo=''):
            if isinstance(elemento, dict):
                for chave, valor in elemento.items():
                    processar_elemento(valor, chave)
            elif isinstance(elemento, list):
                if all(isinstance(item, dict) for item in elemento):
                    normalizado[prefixo] = str([
                        processar_elemento(item, prefixo) 
                        for item in elemento])
                else:
                    normalizado[prefixo] = elemento
            else:
                normalizado[prefixo] = elemento
        processar_elemento(dicionario)
        return normalizado

#==============================================================================
# Funções de validação
#==============================================================================
    
    def valida_presenca_parametros_obrigatorios(self, categoria):
        '''
        Função responsável por validar a existência dos parâmetros obrigatórios
        no arquivo YAML, incluindo parâmetros e subparâmetros.
        '''
        chaves = self.extrair_todas_as_chaves(self.yaml_data[categoria])
        for param in self.parametros_obrigatorios:
            if param not in chaves:
                self.erros.append(f"Categoria: {categoria} - Parâmetro "
                                  f"'{param}' não encontrado no YAML.")

    def valida_parametros_preenchidos(self, categoria):
        '''
        Função responsável por verificar se os parâmetros obrigatórios estão
        preenchidos.
        '''
        try:
            normalizado = self.normalizar_dicionario(self.yaml_data[categoria])
            normalizado_lista = self.encontrar_parametros_com_lista(
                                self.yaml_data[categoria])
            normalizado.update(normalizado_lista)
            for param in self.parametros_preenchidos:
                if not normalizado[param]:
                    self.erros.append(f"Categoria: {categoria} - Parâmetro" 
                                    f"'{param}' não preenchido no YAML.")
        except KeyError as e:
            self.erros.append(f"Categoria: {categoria} - Parâmetro '{param}' "
                              "não preenchido no YAML.")

    def valida_valores_parametros(self):
        '''
        Função responsável por verificar se os valores dos parâmetros estão
        corretos de acordo com o domínio esperado.	
        '''
        for categoria, dados in self.yaml_data.items():
            colunas  = dados.get('colunas') or []
            for coluna in colunas:
                # Verifica se o tipo esta dentro do domínio esperado
                if coluna['tipo'].lower() not in self.valida_tipo:
                    self.erros.append(f"{categoria}: Tipo '{coluna['tipo']}'" 
                        f"inválido na coluna '{coluna['campo']}'.")
                    
                # Verifica se aceita_nulos esta dentro do domínio esperado
                if coluna['aceita_nulos'].lower() not in \
                   self.valida_aceita_nulos:
                    self.erros.append(f"{categoria}: Valor de 'aceita_nulos' " 
                        f"inválido no campo '{coluna['campo']}'.")
                
                # Verifica se o valida_nome_final esta dentro do domínio 
                # esperado
                if coluna['nome_final'].lower() not in self.valida_nome_final:
                    self.erros.append(f"{categoria}: Valor de 'nome_final' " 
                        f"inválido no campo '{coluna['campo']}'.")
                    
                # Verifica se o formato da data esta dentro do domínio esperado
                if coluna['tipo'].lower() == 'data':
                    if coluna['formato'] not in self.valida_formato:
                        self.erros.append(f"{categoria}: Formato de data " 
                            f"inválido no campo '{coluna['campo']}'.")

    def valida_data_ref(self):
        '''
        Validar se a coluna 'data_ref' está configurada corretamente. estando
        presente na declaração de campos e com o tipo correto.
        '''
        for categoria, dados in self.yaml_data.items():
            colunas = dados.get('colunas') or []
            data_ref_valido = any(col['campo'] == dados['data_ref'] and 
                col['tipo'] == 'data' for col in colunas)
            if not data_ref_valido:
                self.erros.append(f"{categoria}: "
                "'data_ref' não configurado corretamente - "
                "campo não declarado ou tipo inválido.")

    def valida_identificador(self):
        '''
        Verifica se existe o preenchimento de ao menos um identificador em
        alguma categoria.
        '''
        identificador_preenchido = any(dados.get('identificador') for 
                                       dados in self.yaml_data.values())
        if not identificador_preenchido:
            self.erros.append("Nenhuma categoria possui 'identificador' "
                              "preenchido.")

    def valida(self):
        try:
            for categoria in self.yaml_data:
                self.valida_presenca_parametros_obrigatorios(categoria)
                self.valida_parametros_preenchidos(categoria)
            self.valida_valores_parametros()
            self.valida_data_ref()
            self.valida_identificador()
            if self.erros:
                print("Yaml com erros! Corrija os seguintes erros encontrados:", 
                    *self.erros, sep="\n")
                return False
            else:
                print("Yaml validado com sucesso!")
                return True
        except:
            print("Erro ao validar o yaml - Estrutura do arquivo no formato "
                  "incorreto para validação. Verifique se existem tabs a mais"
                  " ou a menos antes de cada parâmetro no arquivo yaml.")
            return False