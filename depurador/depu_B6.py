import pandas as pd
import re
from path import DATABASE_PATH
from conexion_bd import get_connection

# --- Archivo ---
database = DATABASE_PATH

# --- Leer archivo ---
df_188 = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).iloc[:, :188]

# --- Validación  ---
registros = []

df_188["cuad_env"] = df_188["cuad_env"].fillna("").astype(str).str.strip()

for idx, fila in df_188.iterrows():
    valor = fila["cuad_env"]

    # Verificar si cumple 
    if re.match(r"^\d{2}$", valor):
        # Si tiene 2 dígitos, convertir a número y validar si es mayor a 12
        if int(valor) > 12:
            registros.append({
                "id_inconsistencia": 10,
                "cod_barra": fila["cod_barra"],
                "columna": "cuad_env",
                "valor": valor,
                "nuevo_valor": "",
                "estado_revision": 0,
                "departamento": fila["departamento"],
                "compuesta": 0,
                "compuesta_values": "",
                "id_usuario": " "
            })
    else:
        registros.append({
            "id_inconsistencia": 10,
            "cod_barra": fila["cod_barra"],
            "columna": "cuad_env",
            "valor": valor,
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

# --- DataFrame ---
df_cuad_env = pd.DataFrame(registros)

print("Filas conflictivas:")
print(df_cuad_env.head())

# --- Exportar  ---
df_cuad_env.to_csv("input/database/B6_datos.txt",sep='\t', index=False, encoding='latin1')

