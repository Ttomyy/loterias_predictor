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
import html
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

       # leer informe de comparación (si existe)
    base = os.path.join(os.path.dirname(__file__), "..")
    info_path = os.path.join(base, "data", "infocompare.txt")
    info_text = ""
    try:
        if os.path.exists(info_path):
            with open(info_path, "r", encoding="utf-8") as f:
                info_text = f.read().strip()
    except Exception:
        info_text = ""

    # escapar HTML para que se muestre de forma segura en <pre>
    info_html = html.escape(info_text)

    body = f"""
    <html>
    <body>
    <h2>Predicción generada</h2>
    <p><b>Prediccion (raw):</b> {pred.get('prediccion')}</p>
    <p><b>Fecha prevista:</b> {pred.get('fecha_predecida')}</p>
    <p><b>Sorteo:</b> {prefix}</p>

    <h3>Informe de comparación</h3>
    <pre style="background:#f7f7f7;padding:10px;border-radius:6px;">{info_html if info_html else 'No hay informe de comparación disponible.'}</pre>

    <hr>
    <p>Este correo se ha generado automáticamente por el pipeline de loterías.</p>
    </body>
    </html>
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
