import traceback
import sys

try:
    # Seu código que pode falhar
    a = 1
    b = 0
    c = a / b
except Exception as e:  # Captura a exceção
    print("Erro encontrado:", str(e))
    texto = traceback.print_exc()