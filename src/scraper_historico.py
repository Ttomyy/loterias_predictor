import requests
from bs4 import BeautifulSoup
from datetime import datetime

def normalizar_fecha(fecha_str: str) -> str:
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }
    try:
        partes = fecha_str.lower().replace(",", "").split()
        dia = int(partes[1])
        mes = meses[partes[2]]
        anio = int(partes[3])
        return datetime(anio, mes, dia).strftime("%Y-%m-%d")
    except Exception:
        return fecha_str

def parsear_fila(fila):
    columnas = fila.find_all("td")
    if not columnas or len(columnas) < 4:
        return None
    fecha = normalizar_fecha(columnas[0].get_text(strip=True))
    numeros = [int(x) for x in columnas[1].get_text(strip=True).split()]
    complementario = int(columnas[2].get_text(strip=True))
    reintegro = int(columnas[3].get_text(strip=True))
    return {
        "juego": "Primitiva",
        "fecha": fecha,
        "numeros": numeros,
        "complementario": complementario,
        "reintegro": reintegro,
    }

def obtener_todos_resultados(url: str):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    filas = soup.find_all("tr")[1:]  # saltamos cabecera
    resultados = [parsear_fila(f) for f in filas if parsear_fila(f)]
    return resultados
