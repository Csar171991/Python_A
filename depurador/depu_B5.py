import pandas as pd
import json
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Archivo ---
database = DATABASE_PATH
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# --- Las primeras 188 columnas ---
df_188 = df.iloc[:, :188].copy()

# --- Campos DIR ---
director_cols = ["nom_director", "ape_director", "dni_director", "tel1_director", "tel2_director"]

# Normalizar
df_188[director_cols] = df_188[director_cols].fillna("").astype(str).apply(lambda col: col.str.strip())

# --- Lista ---
registros_dir = []

# ====================================================
# Validación: solo un director por cor_minedu
# ====================================================
for cor, grupo in df_188.groupby("cor_minedu"):
    directores_unicos = grupo[director_cols].drop_duplicates()

    if len(directores_unicos) > 1: 
        for idx, fila in grupo.iterrows():
            registros_dir.append({
                "id_inconsistencia": 9,
                "cod_barra": fila["cod_barra"],
                "columna": json.dumps({"columnas_dj_director": director_cols}, ensure_ascii=False),
                "valor": json.dumps({"columnas_dj_director": {col: fila[col] for col in director_cols}}, ensure_ascii=False),
                "nuevo_valor": "",
                "estado_revision": 0,
                "departamento": fila["departamento"],
                "compuesta": 0,
                "compuesta_values": "",
                "id_usuario": None
            })

# --- DataFrame ---
df_dir = pd.DataFrame(registros_dir)

print("Inconsistencias detectadas en directores:")
print(df_dir.head())

# Guardar en archivo
#df_dir.to_csv("D:/shared/input/BD_762_FOAR 4P Muestral_1p/B5_datos.txt",sep='\t', index=False, encoding='latin1')

# --- Conexión a BD ---
conn = get_connection()
cursor = conn.cursor()
cursor.fast_executemany = True

# --- Preparar registros ---
records = df_dir.values.tolist()
cols = ",".join(df_dir.columns)
placeholders = ",".join(["?"] * len(df_dir.columns))

sql = f"""
INSERT INTO dbo.tb_detalle_inconsistencia_instrumento_foar_primaria_4to_agrupado_temp ({cols})
VALUES ({placeholders})
"""

# --- Inserción a BD ---
batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    try:
        cursor.executemany(sql, batch)
    except Exception as e:
        print(f"Error en batch {i}-{i+batch_size}")
        print("Ejemplo de registro problemático:", batch[0])
        raise e

# --- Confirmar ---
conn.commit()
cursor.close()
conn.close()

print("Inserción completada")
