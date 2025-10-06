import pandas as pd
import re
import json
from path import DATABASE_PATH
from conexion_bd import get_connection

# --- Parámetros ---
database = DATABASE_PATH
inconsistencia = 22

# --- Regex de validación ---
regex_presenta = re.compile(r"^(1|2|M)?$")
regex_tipo = re.compile(r"^[1-6M]?$")
regex_nomina = re.compile(r"^(1|2|M)?$")

# --- Cargar base ---
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).fillna("")

# --- Lista de registros observados ---
registros = []

# --- Iterar filas ---
for _, fila in df.iterrows():
    presenta = fila["discap_presenta"].strip()
    tipo = fila["discap_tipo"].strip()
    nomina = fila["discap_en_nomina"].strip()

    # --- Validar que tenga al menos un campo de nombre lleno ---
    if not any(fila[col].strip() for col in ["paterno", "materno", "nombre1", "nombre2"]):
        continue

    # --- Validar estructura regex ---
    if (not regex_presenta.fullmatch(presenta)) or (not regex_tipo.fullmatch(tipo)) or (not regex_nomina.fullmatch(nomina)):
        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "columnas_discapacidad": [
                    "discap_presenta", "discap_tipo", "discap_en_nomina"
                ]
            }, ensure_ascii=False),
            "valor": json.dumps({
                "columnas_discapacidad": {
                    "discap_presenta": presenta,
                    "discap_tipo": tipo,
                    "discap_en_nomina": nomina
                }
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })
        continue

    # --- Si discap_presenta está vacío → observar ---
    if presenta == "":
        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "columnas_discapacidad": [
                    "discap_presenta", "discap_tipo", "discap_en_nomina"
                ]
            }, ensure_ascii=False),
            "valor": json.dumps({
                "columnas_discapacidad": {
                    "discap_presenta": presenta,
                    "discap_tipo": tipo,
                    "discap_en_nomina": nomina
                }
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })
        continue

    # --- Si presenta es “M” → observar ---
    if presenta == "M":
        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "columnas_discapacidad": [
                    "discap_presenta", "discap_tipo", "discap_en_nomina"
                ]
            }, ensure_ascii=False),
            "valor": json.dumps({
                "columnas_discapacidad": {
                    "discap_presenta": presenta,
                    "discap_tipo": tipo,
                    "discap_en_nomina": nomina
                }
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })
        continue

    # --- Si presenta = 1 → tipo y nomina deben estar llenos ---
    if presenta == "1" and (tipo == "" or nomina == ""):
        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "columnas_discapacidad": [
                    "discap_presenta", "discap_tipo", "discap_en_nomina"
                ]
            }, ensure_ascii=False),
            "valor": json.dumps({
                "columnas_discapacidad": {
                    "discap_presenta": presenta,
                    "discap_tipo": tipo,
                    "discap_en_nomina": nomina
                }
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })

    # --- Si presenta = 2 → tipo y nomina deben estar vacíos ---
    elif presenta == "2" and (tipo != "" or nomina != ""):
        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "columnas_discapacidad": [
                    "discap_presenta", "discap_tipo", "discap_en_nomina"
                ]
            }, ensure_ascii=False),
            "valor": json.dumps({
                "columnas_discapacidad": {
                    "discap_presenta": presenta,
                    "discap_tipo": tipo,
                    "discap_en_nomina": nomina
                }
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })

# --- Convertir a DataFrame para exportar o insertar ---
df_discapacidad = pd.DataFrame(registros)
print("Filas con inconsistencias en discapacidad:")
print(df_discapacidad.head())

# --- Exportar ---
df_discapacidad.to_csv("input/database/D4_datos.txt", sep='\t', index=False, encoding='latin1')

