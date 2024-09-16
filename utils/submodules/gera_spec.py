from pyspark.sql import SparkSession

class Spec():
    def __init__(self, args):
        self.args = args
        self.spark = SparkSession.builder.getOrCreate()
        self.path_sot =  args['caminho_sot_atual']
        self.path_spec =  args['CAMINHO_BUCKET'] + '\\' + '04_SPEC'
        # importa o arquivo SOT
        self.df = self.spark.read.parquet(self.path_sot)
        self.df_base = self.spark.read.csv(args['caminho_base'], 
                                           header=True, sep=';')
        
    
    def etl(self):
        # gera o arquivo SPEC
        self.df.write.parquet(self.path_spec)
        return self.args

