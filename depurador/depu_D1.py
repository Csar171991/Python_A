import pandas as pd
import json
from conexion_bd import get_connection
from path import DATABASE_PATH
import re

# --- Lista  ---
registros = []

# -- Archivo --
database = DATABASE_PATH

# Cargar 
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

regex_dni = re.compile(r"^\d{2}$")  

# Normalizar
df["cor_est"] = df["cor_est"].fillna("").astype(str).str.strip()
df["asis_dia1_lec"] = df["asis_dia1_lec"].fillna("0").astype(str).str.strip()
df["asis_dia1_mat"] = df["asis_dia1_mat"].fillna("0").astype(str).str.strip()
df["asis_dia2_mat"] = df["asis_dia2_mat"].fillna("0").astype(str).str.strip()
df["asis_dia2_lec"] = df["asis_dia2_lec"].fillna("0").astype(str).str.strip()

# Filtrar solo estudiantes con asistencia en cualquiera de los campos
df_presentes = df[
    (df["asis_dia1_lec"] == "1") |
    (df["asis_dia1_mat"] == "1") |
    (df["asis_dia2_mat"] == "1") |
    (df["asis_dia2_lec"] == "1")
]


# --- Correlativos inválidos ---
for idx, fila in df_presentes.iterrows():
    valor = str(fila["cor_est"]).strip()
    # Si no cumple con 8 dígitos, registrar inconsistencia
    if not re.fullmatch(r"\d{2}", valor):
        registros.append({
            "id_inconsistencia": 15,
            "cod_barra": fila["cod_barra"],
            "columna": "cor_est",
            "valor": valor,
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })


# DataFrame final
cor_est_inconsistentes = pd.DataFrame(registros)

print("Inconsistencias de correlativo estudiante:")
print(cor_est_inconsistentes.head())

# Guardar
cor_est_inconsistentes.to_csv("input/database/D1_datos.txt",sep="\t",index=False,encoding="latin1")