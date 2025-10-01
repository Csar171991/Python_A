import pandas as pd
import re
from path import DATABASE_PATH

# --- Lista ---
registros = []

# -- Archivo --
database = DATABASE_PATH

# Cargar toda la base
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# Normalizar 
df["len_mater"] = df["len_mater"].fillna("").astype(str).str.strip()
df["paterno"] = df["paterno"].fillna("").astype(str).str.strip()
df["materno"] = df["materno"].fillna("").astype(str).str.strip()
df["nombre1"] = df["nombre1"].fillna("").astype(str).str.strip()
df["nombre2"] = df["nombre2"].fillna("").astype(str).str.strip()

# --- Regex válido para len_mater: 1-4 o M ---
regex_len_mater = re.compile(r"^[1-4]$")

# --- Filtrar filas donde hay alguno de ellos lleno ---
df_filtrado = df[
    (df["paterno"] != "") |
    (df["materno"] != "") |
    (df["nombre1"] != "") |
    (df["nombre2"] != "")
]

# --- Validación---
for idx, fila in df_filtrado.iterrows():
    valor = str(fila["len_mater"]).strip().upper()
    
    # Si está vacío o no
    if not regex_len_mater.fullmatch(valor):
        registros.append({
            "id_inconsistencia": 17,   
            "cod_barra": fila.get("cod_barra", ""),
            "columna": "len_mater",
            "valor": valor,
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila.get("departamento", ""),
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

# DataFrame final con inconsistencias
len_mater_inconsistentes = pd.DataFrame(registros)

print("Inconsistencias de campo LEN_MATER:")
print(len_mater_inconsistentes.head())

# Guardar en archivo
len_mater_inconsistentes.to_csv("input/database/D3_datos.txt", sep="\t", index=False, encoding="latin1")
