# debug_highlight.py
from pathlib import Path
import html, re

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


text = Path("data/infocompare.txt").read_text(encoding="utf-8") if Path("data/infocompare.txt").exists() else "aciertos: 3\notra linea"
print("----- RAW file -----")
print(text)
print("----- HTML highlighted -----")
print(highlight_aciertos(text))
