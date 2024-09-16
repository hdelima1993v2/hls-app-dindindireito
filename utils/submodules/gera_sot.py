from pyspark.sql.types import StructType, StringType, DoubleType, DateType
from pyspark.sql.functions import max, lit
from utils.submodules.aplicacao_principal import DinDinDireito
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
import pandas as pd

class SoT(DinDinDireito):
    def __init__(self, args):
        super(SoT, self).__init__(args)
        self.args = args
        self.spark = SparkSession.builder.getOrCreate()
        self.pd_SOR = self.pd_clean_df.copy()
        self.spk_schema = self.transforma_schema()
        self.spk_clean_df = self.spark.createDataFrame(self.pd_SOR,
                                                   self.spk_schema)
        self.data_ref = self.define_data_ref()
        self.particoes = ['tipo', 'data_ref']
        self.caminho_sot_atual = (self.path_sot + '\\SOT_' + self.tipo + '_' 
                                  + self.data_ref  + '.parquet')
        self.args['tipo'] = self.tipo
        self.args['ext_sor'] = self.ext_sor
        self.args['data_ref'] = self.data_ref
        self.args['path_sor'] = self.path_sot
        self.args['path_arq'] = self.path_arq
        self.args['caminho_sot_atual'] = self.caminho_sot_atual

    def transforma_schema(self):
        '''
        Função responsável por transformar o schema do pandas para o schema do
        pyspark
        '''
        schema = StructType()
        for item in self.schema:
            # traduz para o pyspark o tipo de dado na planilha
            if item['tipo']=='data':
                item['tipo']= DateType()
                self.pd_SOR[item['campo']] = pd.to_datetime(
                    self.pd_SOR[item['campo']], 
                    format = item['formato'], errors='coerce')
                
            elif item['tipo']=='texto':
                item['tipo']= StringType()

            elif item['tipo']=='numero':
                item['tipo']= DoubleType()
                self.pd_SOR[item['campo']] = pd.to_numeric(
                    self.pd_SOR[item['campo']], errors='coerce')
            
            # traduz se o campo aceita nulos ou não 
            if item['aceita_nulos'].lower() == 's':
                item['aceita_nulos'] = True
            else:
                campo = item['campo']
                item['aceita_nulos'] = False
                self.pd_SOR = self.pd_SOR.dropna(subset = campo)
            
            # adiciona traduções ao schema definitivo
            schema.add(item['campo'], item['tipo'], item['aceita_nulos'])
        return schema

    def cria_campos_constantes(self):
        '''
        Função responsável por criar campos constantes no dataframe
        '''
        # cria coluna com o valor self.tipo no dataframe
        self.spk_clean_df = self.spk_clean_df.withColumn('tipo', 
                                                         lit(self.tipo))
        # cria coluna com o valor self.data_ref no dataframe
        self.spk_clean_df = self.spk_clean_df.withColumn('data_ref', 
                                                         lit(self.data_ref))
        return self.spk_clean_df

    def define_data_ref(self):
        '''
        Função responsável por definir a data de referência do dataframe
        '''
        # Encontrar o maior valor na coluna de data
        df_resultado = self.spk_clean_df.agg(max(self.campo_ref)\
                                             .alias("data_ref"))
        max_data_value = df_resultado.collect()[0]["data_ref"]
        return max_data_value.strftime('%Y%m')
    
    def salva_arq(self):
        '''
        Função responsável por salvar o arquivo SOT
        '''
        self.spk_clean_df.write.parquet(
            path = self.caminho_sot_atual                          
            , mode='overwrite'
              , partitionBy = self.particoes)
        
        # # salve o self.spk_clean_df como csv
        # df_coalesced = self.spk_clean_df.coalesce(1)
        # df_coalesced.write.csv(
        #     path = self.caminho_sot_atual                          
        #     , mode='overwrite')

    def renomeia_campos(self):
        '''
        Função responsável por renomear os campos do dataframe
        '''
        for item in self.schema:
            self.spk_clean_df = self.spk_clean_df.withColumnRenamed(
                item['campo'], item['nome_final'])

    def etl(self):

        # cria campos constantes
        self.cria_campos_constantes()

        # salva arquivo SOT
        self.salva_arq()        

        return self.args
