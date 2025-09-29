from pymongo import MongoClient, UpdateOne

def insertar_historico(resultados, uri="mongodb://localhost:27017/", db_name="loterias", coleccion="resultados_loterias"):
    cliente = MongoClient(uri)
    db = cliente[db_name]
    col = db[coleccion]

    operaciones = []
    for r in resultados:
        operaciones.append(
            UpdateOne(
                {"juego": "Primitiva", "fecha": r["fecha"]},
                {"$set": r},
                upsert=True
            )
        )

    if operaciones:
        resultado = col.bulk_write(operaciones, ordered=False)
        print(f"{resultado.upserted_count + resultado.modified_count} registros insertados/actualizados en MongoDB")
    else:
        print("No hay datos para insertar.")
