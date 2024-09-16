
from utils.submodules.validacao_parametros import validador_yaml
from utils.submodules.gera_spec import Spec
from utils.submodules.gera_sot import SoT
from utils.utils import funcoes
import sys

def main():
    args = funcoes.coleta_argumentos(sys.argv)

    # valida se o arquivo yaml é válido
    if not validador_yaml(args['PATH_YAML']).valida():
        return
    
    # tratamento dos dados - Gera SoT
    args = SoT(args).etl()

    # tratamento dos dados - Gera Spec
    #args = Spec(args).etl()

    # mover arquivo para pasta de processados
    funcoes.move_SoR(args)
        
    print('Arquivo processado com sucesso!')

if __name__ == "__main__":
    main()



