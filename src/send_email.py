"""send_email_improved.py
Versión mejorada del script de envío de correo.
Cambios principales:
- Manejo robusto de destinatarios desde EMAIL_TO (cadena con comas/;)
- Logs con logging
- Manejo de errores más claro
- Resalta las líneas que contienen "aciertos" en rojo y negrita dentro del informe de comparación
- Mejor limpieza de rutas y validaciones
- Soporta ejecución desde CLI con --prefix
"""

import os
import re
import logging
import argparse
import html
from datetime import datetime
from typing import List

import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from dotenv import load_dotenv
import html
load_dotenv() 

# Config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# === CONFIG desde entorno ===
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
#EMAIL_USER = os.getenv("EMAIL_USER")
#EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO_RAW = os.getenv("EMAIL_TO", "")  # puede ser 'a@x.com,b@y.com' o 'a@x.com; b@y.com'
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO =   os.getenv("EMAIL_TO","" )
PRED_FILE = os.path.join(os.path.dirname(__file__), "..","data", "predicciones.csv")


# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
#PRED_FILE = os.path.join(BASE_DIR, "data", "predicciones.csv")
INFO_COMPARE = os.path.join(BASE_DIR, "data", "infocompare.txt")


def parse_recipients(raw: str) -> List[str]:
    """Parsea una cadena de destinatarios separados por comas o punto y coma.
    Devuelve una lista limpia.
    """
    if not raw:
        return []
    parts = re.split(r"[,;]", raw)
    recipients = [p.strip() for p in parts if p and p.strip()]
    return recipients


def highlight_aciertos(text: str) -> str:
    """
    Escapa el texto y resalta (negrita + rojo) cualquier línea que contenga
    la palabra 'aciertos' (case-insensitive). Resalta la línea completa
    para cubrir formatos como: "Aciertos: [34] (1)".
    """
    if not text:
        return ""

    # Primero escapamos el texto para evitar inyección HTML
    escaped = html.escape(text)

    # Patrón que captura líneas enteras que contienen la palabra 'aciertos'
    pattern = re.compile(r'^(.*\baciertos\b.*)$', re.IGNORECASE | re.MULTILINE)

    def repl(match: re.Match) -> str:
        line = match.group(1)
        # envolvemos la línea ya escapada en strong + estilo inline
        return f'<strong style="color:red;">{line}</strong>'

    highlighted = pattern.sub(repl, escaped)
    return highlighted



def load_last_prediction() -> dict:
    logger.debug("Cargando últimas predicciones desde %s", PRED_FILE)
    if not os.path.exists(PRED_FILE):
        raise FileNotFoundError(f"No existe {PRED_FILE}")

    df = pd.read_csv(PRED_FILE)
    if df.empty:
        raise ValueError("El archivo de predicciones está vacío.")

    last = df.iloc[-1].to_dict()
    return last


def load_info_compare() -> str:
    try:
        if os.path.exists(INFO_COMPARE):
            with open(INFO_COMPARE, "r", encoding="utf-8") as f:
                return f.read().strip()
    except Exception as e:
        logger.warning("No se pudo leer %s: %s", INFO_COMPARE, e)
    return ""


def build_body(pred: dict, prefix: str) -> str:
    info_text = load_info_compare()
    info_html = highlight_aciertos(info_text) if info_text else ""

    pred_raw = pred.get("prediccion", "")
    fecha_predecida = pred.get("fecha_predecida", "")

    body = f"""
    <html>
    <body>
    <h2>Predicción generada</h2>
    <p><b>Predicción (raw):</b> {html.escape(str(pred_raw))}</p>
    <p><b>Fecha prevista:</b> {html.escape(str(fecha_predecida))}</p>
    <p><b>Sorteo:</b> {html.escape(prefix)}</p>

    <h3>Informe de comparación</h3>
    <pre style="background:#f7f7f7;padding:10px;border-radius:6px;">{info_html if info_html else 'No hay informe de comparación disponible.'}</pre>

    <hr>
    <p>Este correo se ha generado automáticamente por el pipeline de loterías.</p>
    </body>
    </html>
    """
    return body


def send_email(pred: dict, prefix: str, dry_run: bool = False) -> None:
    recipients = parse_recipients(EMAIL_TO_RAW)
    if not recipients:
        raise ValueError("No hay destinatarios configurados en EMAIL_TO")

    if not EMAIL_USER or not EMAIL_PASS:
        raise ValueError("Faltan credenciales SMTP: EMAIL_USER y/o EMAIL_PASS no están definidas en el entorno")

    fecha_pred = str(pred.get("fecha_predecida", "")).capitalize()
    current_date = datetime.now().strftime("%Y-%m-%d")
    subject = f"Predicción Lotería {fecha_pred} - {current_date}"

    body = build_body(pred, prefix)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    # Adjuntar archivo CSV de predicciones
    if os.path.exists(PRED_FILE):
        try:
            with open(PRED_FILE, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{os.path.basename(PRED_FILE)}"',
                )
                msg.attach(part)
        except Exception as e:
            logger.warning("No se pudo adjuntar %s: %s", PRED_FILE, e)
    else:
        logger.info("No existe archivo de predicciones para adjuntar: %s", PRED_FILE)

    if dry_run:
        logger.info("Dry run: no se enviará el correo. Destinatarios: %s", recipients)
        return

    # Envío real
    logger.info("Conectando a SMTP %s:%d", SMTP_SERVER, SMTP_PORT)
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, recipients, msg.as_string())
            logger.info("Correo enviado a %s", recipients)
    except Exception as e:
        logger.exception("Error enviando correo: %s", e)
        raise


def main():
    parser = argparse.ArgumentParser(description="Enviar email con la última predicción")
    parser.add_argument("--prefix", required=False, default=os.getenv("PREFIX", "bonoloto"), help="Nombre del sorteo: bonoloto|loteria|...")
    parser.add_argument("--dry-run", action="store_true", help="No envía el correo, solo simula y muestra logging")
    args = parser.parse_args()

    try:
        pred = load_last_prediction()
        send_email(pred, args.prefix, dry_run=args.dry_run)
    except Exception as e:
        logger.exception("Fallo al enviar correo: %s", e)
        print("Error enviando correo:", e)


if __name__ == "__main__":
    main()
