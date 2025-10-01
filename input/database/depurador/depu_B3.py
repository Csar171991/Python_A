import pandas as pd
import json
import re
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Leer archivo ---
database = DATABASE_PATH
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# --- Tomar 188 columnas ---
df_188 = df.iloc[:, :188]

# --- Regex ---
regex_valido = re.compile(r"^[12M ]$")

# --- Función para bloques ---
def procesar_bloque(df, columnas, nombre_bloque):
    """
    Procesa un bloque de columnas y devuelve un DataFrame con las inconsistencias.
    """
    # Limpiar
    bloque = df[columnas].astype(str).apply(lambda col: col.str.strip())

    # Regla: todos vacíos o todos llenos
    todos_vacios = (bloque == "").all(axis=1)
    todos_llenos = (bloque != "").all(axis=1)
    inconsistentes = ~(todos_vacios | todos_llenos)

    # Detectar conflictos 
    conflictos = df[inconsistentes]

    registros = []

    for idx, fila in conflictos.iterrows():
        valor = {col: fila[col] for col in columnas}
        registros.append({
            "id_inconsistencia": 7,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({nombre_bloque: columnas}, ensure_ascii=False),
            "valor": json.dumps({nombre_bloque: valor}, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })

    # --- Validación regex extra ---
    for idx, fila in df.iterrows():
        for col in columnas:
            val = str(fila[col]).strip()
            if val != "" and not regex_valido.fullmatch(val):
                registros.append({
                    "id_inconsistencia": 7,
                    "cod_barra": fila["cod_barra"],
                    "columna": col,
                    "valor": val,
                    "nuevo_valor": "",
                    "estado_revision": 0,
                    "departamento": fila["departamento"],
                    "compuesta": 0,
                    "compuesta_values": "",
                    "id_usuario": None
                })

    return pd.DataFrame(registros)

# --- Definición de bloques ---
director_cols = ["dir01", "dir02", "dir03", "dir04", "dir05_1", "dir05_2", "dir05_2_d1", "dir05_2_d2", "dir06", "dir07", "dir08", "dir09", "dir09_01", "dir09_02"]
docente_cols = ["doc01","doc02","doc03","doc04","doc04_1","doc04_2","doc05","doc06","doc07","doc08","doc09","doc10"]
aplicador_cols = ["apli01","apli02","apli03","apli04","apli05","apli06","apli07","apli08","apli09","apli10","apli11","apli12","apli13","apli14","apli16"]
asistente_cols = ["asis01","asis02","asis03","asis04","asis04_1","asis05","asis06","asis07","asis08","asis09","asis10","asis11","asis12","asis12_1","asis13","asis13_1","asis14"]

# --- Procesar cada bloque ---
df_director  = procesar_bloque(df_188, director_cols, "columnas_dj_director")
df_docente   = procesar_bloque(df_188, docente_cols, "columnas_dj_docente")
df_aplicador = procesar_bloque(df_188, aplicador_cols, "columnas_dj_aplicador")
df_asistente = procesar_bloque(df_188, asistente_cols, "columnas_dj_asistente")

# --- Unir ---
df_final = pd.concat([df_director, df_docente, df_aplicador, df_asistente], ignore_index=True)

# --- Conexión a BD ---
conn = get_connection()
cursor = conn.cursor()
cursor.fast_executemany = True

# --- Preparar ---
records = df_final.values.tolist()
cols = ",".join(df_final.columns)
placeholders = ",".join(["?"] * len(df_final.columns))

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


