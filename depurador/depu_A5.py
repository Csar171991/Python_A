import pandas as pd
import re
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Archivo ---
df = pd.read_csv(DATABASE_PATH, delimiter='\t', encoding='latin1', dtype=str)

# Normalizar
df = df.fillna("").astype(str)
df["cor_est"] = df["cor_est"].str.strip()
df["cod_mod7"] = df["cod_mod7"].str.strip()

# Limpiar
campos_nombres = ["paterno", "materno", "nombre1", "nombre2"]
for campo in campos_nombres:
    df[campo] = df[campo].str.upper().str.strip()

# Crear concatenado 
df["nombre_completo"] = (
    df["paterno"] + " " +
    df["materno"] + " " +
    df["nombre1"] + " " +
    df["nombre2"]
).str.replace(r"\s+", " ", regex=True).str.strip()

# --- Regex ---
regex_cor = re.compile(r"^\d{2}$")

# --- Lista ---
registros = []

# =======================================================
# 1. Validación duplicado cor_est por cod_mod7
# =======================================================
df_validos = df[(df["cor_est"].apply(lambda x: bool(regex_cor.match(x)))) & (df["cor_est"] != "")]

duplicados_cor = df_validos[df_validos.duplicated(subset=["cod_mod7", "cor_est"], keep=False)]

for idx, fila in duplicados_cor.iterrows():
    registros.append({
        "id_inconsistencia": 19,
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
# 2. Validación duplicado de nombres completos en toda la base
# =======================================================
df_validos_nombres = df[df["nombre_completo"] != ""]

duplicados_nombres = df_validos_nombres[df_validos_nombres.duplicated(subset=["nombre_completo"], keep=False)]

for idx, fila in duplicados_nombres.iterrows():
    registros.append({
        "id_inconsistencia": 19,
        "cod_barra": fila["cod_barra"],
        "columna": "apellidosynombres",
        "valor": fila["nombre_completo"],
        "nuevo_valor": "",
        "estado_revision": 0,
        "departamento": fila["departamento"],
        "compuesta": 0,
        "compuesta_values": "",
        "id_usuario": " "
    })

# --- Consolidar ---
df_inconsistencias = pd.DataFrame(registros)

print("Filas con duplicados en cor_est o nombres completos:")
print(df_inconsistencias.head())

# --- Exportar ---
df_inconsistencias.to_csv("input/database/A5_datos.txt", sep="\t", index=False, encoding="utf-8")


