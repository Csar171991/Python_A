import pandas as pd
import re
import os
import json
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Cargar archivo ---
df = pd.read_csv(DATABASE_PATH, delimiter='\t', encoding='latin1', dtype=str).fillna("").astype(str)

# --- Normalizar campos clave ---
df["cor_est"] = df["cor_est"].str.strip()
df["cod_mod7"] = df["cod_mod7"].str.strip().str.upper()

# --- Normalizar nombres ---
campos_nombres = ["paterno", "materno", "nombre1", "nombre2"]
for campo in campos_nombres:
    df[campo] = df[campo].str.upper().str.strip()

# --- Concatenar nombres completos ---
df["nombre_completo"] = (
    df["paterno"] + " " + df["materno"] + " " + df["nombre1"] + " " + df["nombre2"]
).str.replace(r"\s+", " ", regex=True).str.strip()

# --- Regex para cor_est ---
regex_cor = re.compile(r"^\d{2}$")

# --- Lista para inconsistencias ---
registros = []
registros1 = []
# =======================================================
# 1. Duplicados de cor_est por cod_mod7 (incluye vacíos)
# =======================================================
df["cor_est_valido"] = df["cor_est"].apply(lambda x: bool(regex_cor.match(x)))

duplicados_cor = df[df.duplicated(subset=["cod_mod7", "cor_est"], keep=False)]

for _, fila in duplicados_cor.iterrows():
    registros.append({
        "id_inconsistencia": 16,
        "cod_barra": fila["cod_barra"],
        "columna": "cor_est",
        "valor": fila["cor_est"],
        "nuevo_valor": "",
        "estado_revision": 0,
        "departamento": fila["departamento"],
        "compuesta": 0,
        "compuesta_values": "",
        "id_usuario": " "
    })

# =======================================================
# 2. Duplicados de nombres completos (JSON con columnas_duplicados)
# =======================================================
duplicados_nombres = df[df.duplicated(subset=["nombre_completo"], keep=False) & (df["nombre_completo"] != "")]

nombre_bloque = "columnas_duplicados"
columnas = ["paterno", "materno", "nombre1", "nombre2"]

for _, fila in duplicados_nombres.iterrows():
    valor = {campo: fila[campo] for campo in columnas}

    registros1.append({
        "id_inconsistencia": 17,
        "cod_barra": fila["cod_barra"],
        "columna": json.dumps({nombre_bloque: columnas}, ensure_ascii=False),
        "valor": json.dumps({nombre_bloque: valor}, ensure_ascii=False),
        "nuevo_valor": "",
        "estado_revision": 0,
        "departamento": fila["departamento"],
        "compuesta": 0,
        "compuesta_values": "",
        "id_usuario": ""
    })

# --- Consolidar inconsistencias ---
df_inconsistencias = pd.DataFrame(registros)
df_inconsistencias1 = pd.DataFrame(registros1)

# --- Mostrar resultado ---
print("Total inconsistencias detectadas:", len(df_inconsistencias))
print(df_inconsistencias.head())


# --- Exportar resultado ---
df_inconsistencias.to_csv("input/database/A5_datos.txt", sep="\t", index=False, encoding="latin1")
df_inconsistencias1.to_csv("input/database/A5_datos1.txt", sep="\t", index=False, encoding="latin1")



