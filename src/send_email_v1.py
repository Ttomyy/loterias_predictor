import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os
import argparse
from dotenv import load_dotenv
load_dotenv()  # Cargar variables de entorno desde .env
# === CONFIG ===
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO =   os.getenv("EMAIL_TO","" )
PRED_FILE = os.path.join(os.path.dirname(__file__), "..","data", "predicciones.csv")



def load_last_prediction():
    if not os.path.exists(PRED_FILE):
        raise FileNotFoundError(f"No existe {PRED_FILE}")
    
    df = pd.read_csv(PRED_FILE)
    if df.empty:
        raise ValueError("El archivo de predicciones está vacío.")
    
    last = df.iloc[-1].to_dict()
    return last

def send_email(pred, prefix: str):
    current_date = datetime.now().strftime("%Y-%m-%d")

    subject = f"Predicción Lotería {pred.get('"fecha_predecida"', '').capitalize()} - {current_date}"

    print(pred)
    body = f"""
    <h2>Predicción generada</h2>
    <p><b>Prediccion:</b> {pred.get('prediccion')}</p>
    <p><b>Fecha:</b> {pred.get('fecha_predecida')}</p>
    <p><b>Sorteo:</b> {prefix}</p>
    <hr>
    <p>Este correo se ha generado automáticamente por el pipeline de loterías.</p>
    """
    
    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(EMAIL_TO)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
        # Adjuntar el archivo
    with open(PRED_FILE, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(PRED_FILE)}"',
        )
        msg.attach(part)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
        print(f"Correo enviado a {EMAIL_TO}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", required=True, help="Nombre del sorteo: primitiva o bonoloto")
    args = parser.parse_args()
    try:
        pred = load_last_prediction()
        #PREFIX = os.environ.get("PREFIX", "bonoloto")  # o pásalo como argumento

        send_email(pred, args.prefix)
    except Exception as e:
        print("Error enviando correo:", e)
