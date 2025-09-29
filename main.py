from src.scraper import obtener_ultimo_resultado, obtener_todos_resultados
from src.db import insertar_resultado
from src.db_2 import insertar_historico

def main():
    print("=== Primitiva Predictor ===")
    print("1. Descargar e insertar último resultado")
    print("2. Descargar e insertar histórico completo")
    opcion = input("Elige una opción (1/2): ")

    if opcion == "1":
        ultimo = obtener_ultimo_resultado()
        if ultimo:
            insertar_resultado(ultimo)
    elif opcion == "2":
        historico = obtener_todos_resultados()
        if historico:
            insertar_historico(historico)
    else:
        print("Opción no válida.")

if __name__ == "__main__":
    main()
