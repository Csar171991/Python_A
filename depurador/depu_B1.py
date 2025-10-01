import pandas as pd
from conexion_bd import get_connection
from path import DATABASE_PATH


database = DATABASE_PATH

# 188 columnas
df_188 = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).iloc[:, :188]

# --- Validación columna TURNO ---
registros = []

# columna turno
df_188["turno"] = df_188["turno"].fillna("").astype(str).str.strip()

for idx, fila in df_188.iterrows():
    if fila["turno"] == "" or fila["turno"] == "M":
        registros.append({
            "id_inconsistencia": 2,   
            "cod_barra": fila["cod_barra"],
            "columna": "turno",           
            "valor": fila["turno"],       
            "nuevo_valor": "",         
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",   
            "id_usuario": " "      
        })

# Pasar a DataFrame
df_turno = pd.DataFrame(registros)

print("Filas conflictivas en columna TURNO:")
print(df_turno.head())

df_turno.to_csv("D:/shared/input/BD_762_FOAR 4P Muestral_1p/B1_datos.txt", sep='\t', index=False, encoding='latin1')


