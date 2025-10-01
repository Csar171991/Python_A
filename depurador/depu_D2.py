import pandas as pd
import re
from path import DATABASE_PATH

# --- Lista ---
registros = []

# -- Archivo --
database = DATABASE_PATH

# Cargar
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# Normalizar campos
df["sexo"] = df["sexo"].fillna("").astype(str).str.strip()
df["paterno"] = df["paterno"].fillna("").astype(str).str.strip()
df["materno"] = df["materno"].fillna("").astype(str).str.strip()
df["nombre1"] = df["nombre1"].fillna("").astype(str).str.strip()
df["nombre2"] = df["nombre2"].fillna("").astype(str).str.strip()

# --- Regex válido para sexo ---
regex_sexo = re.compile(r"^(1|2)$")

# --- Filtrar filas para ver si está lleno alguno---
df_filtrado = df[
    (df["paterno"] != "") |
    (df["materno"] != "") |
    (df["nombre1"] != "") |
    (df["nombre2"] != "")
]

# --- Validación---
for idx, fila in df_filtrado.iterrows():
    valor = str(fila["sexo"]).strip()
    
    # Si sexo no es 1 ni 2 -> inconsistencia
    if not regex_sexo.fullmatch(valor):
        registros.append({
            "id_inconsistencia": 16, 
            "cod_barra": fila.get("cod_barra", ""),
            "columna": "sexo",
            "valor": valor,
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila.get("departamento", ""),
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

# DataFrame final con inconsistencias
sexo_inconsistentes = pd.DataFrame(registros)

print("Inconsistencias de campo SEXO:")
print(sexo_inconsistentes.head())

# Guardar en archivo
sexo_inconsistentes.to_csv("input/database/D2_datos.txt", sep="\t", index=False, encoding="latin1")

