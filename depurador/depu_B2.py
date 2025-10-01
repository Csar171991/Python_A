import pandas as pd
import json
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Leer archivo ---
database = DATABASE_PATH

df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# --- Tomar solo las primeras 188 columnas ---
df_188 = df.iloc[:, :188]

# --- Bloques por posición ---
bloque_A = df_188.iloc[:, 16:37].astype(str).apply(lambda col: col.str.strip())
bloque_B = df_188.iloc[:, 169:188].astype(str).apply(lambda col: col.str.strip())

# --- Guardar cabeceras ---
bloque_A_cols = bloque_A.columns.tolist()
bloque_B_cols = bloque_B.columns.tolist()

# --- Verificar vacíos ---
A_vacio = (bloque_A == "").all(axis=1)
B_vacio = (bloque_B == "").all(axis=1)

# --- Inconsistencia: ambos no vacíos ---
inconsistencia = (~A_vacio) & (~B_vacio)

conflictos = df_188[inconsistencia]

# --- Armar resultado ---
resultado = pd.concat(
    [
        df.loc[conflictos.index, ["cod_barra", "departamento"]],
        bloque_A.loc[conflictos.index],
        bloque_B.loc[conflictos.index]
    ],
    axis=1
)

# --- JSON de cabeceras ---
cabeceras_json = {
    "columnas_dj_director": bloque_A_cols,
    "columnas_constancia_no_aplicacion": bloque_B_cols
}

# --- Preparar datos para inserción ---
registros = []
for idx, fila in resultado.iterrows():
    valor = {
        "columnas_dj_director": {col: fila[col] for col in bloque_A_cols},
        "columnas_constancia_no_aplicacion": {col: fila[col] for col in bloque_B_cols}
    }
    registros.append({
        "id_inconsistencia": 1,
        "cod_barra": fila["cod_barra"],
        "columna": json.dumps(cabeceras_json, ensure_ascii=False),
        "valor": json.dumps(valor, ensure_ascii=False),
        "nuevo_valor": "",
        "estado_revision": 0,
        "departamento": fila["departamento"],
        "compuesta": 0,
        "compuesta_values": "",
        "id_usuario": None
    })

df_datos = pd.DataFrame(registros)
# Solo columnas que deben ser string (todas menos id_usuario)
cols_sin_usuario = [c for c in df_datos.columns if c != "id_usuario"]

df_datos[cols_sin_usuario] = (
    df_datos[cols_sin_usuario]
    .fillna("")
    .astype(str)
)

# Mantener
df_datos["id_usuario"] = df_datos["id_usuario"].where(df_datos["id_usuario"].notna(), None)
#df_datos = df_datos.fillna("").astype(str) 

# --- Conexión a bd ---
conn = get_connection()
cursor = conn.cursor()
cursor.fast_executemany = True

records = df_datos.values.tolist()
cols = ",".join(df_datos.columns)
placeholders = ",".join(["?"] * len(df_datos.columns))

sql = f"""
INSERT INTO dbo.tb_detalle_inconsistencia_instrumento_foar_primaria_4to_agrupado_temp ({cols}) 
VALUES ({placeholders})
"""

# --- Inserción a bd ---
batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    for j, record in enumerate(batch):
        try:
            cursor.execute(sql, record)
        except Exception as e:
            print(f"Error en batch {i}-{i+batch_size}, fila {j}")
            print(record)
            raise e

conn.commit()
cursor.close()
conn.close()

print("Inserción completada")



