import pandas as pd
import json
import re
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Leer archivo ---
database = DATABASE_PATH
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# --- Campos especiales ---
campo_doc10 = "doc10"
campos_grados = ["doc_grado_1", "doc_grado_2", "doc_grado_3", "doc_grado_5", "doc_grado_6"]

# Normalizar
df[[campo_doc10] + campos_grados] = df[[campo_doc10] + campos_grados].fillna("").astype(str).apply(lambda col: col.str.strip())

# Regex válido
patron_doc10 = re.compile(r"^[12M ]$")

# --- Lista ---
registros = []

# Validaciones
for idx, fila in df.iterrows():
    valor_doc10 = fila[campo_doc10]
    valores_grados = {col: fila[col] for col in campos_grados}
    grados_llenos = any(v.strip() != "" for v in valores_grados.values())

    inconsistente = False

    # 1. Valor fuera de regex
    if not patron_doc10.match(valor_doc10):
        inconsistente = True

    # 2. Caso vacío/blanco
    elif valor_doc10.strip() == "":
        inconsistente = True

    # 3. Caso "1" o "M" 
    elif valor_doc10 in ["1", "M"] and not grados_llenos:
        inconsistente = True

    # 4. Caso "2"
    elif valor_doc10 == "2" and grados_llenos:
        inconsistente = True

    if inconsistente:
        registros.append({
            "id_inconsistencia": 11,
            "cod_barra": fila["cod_barra"],
            "columna": json.dumps({
                "docente_comparte_grados": [campo_doc10],
                "docente_grados_disponibles": campos_grados
            }, ensure_ascii=False),
            "valor": json.dumps({
                "docente_comparte_grados": {campo_doc10: valor_doc10},
                "docente_grados_disponibles": valores_grados
            }, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": None
        })

# --- DataFrame final ---
df_inconsistencias = pd.DataFrame(registros)

print("Inconsistencias detectadas en docentes-grados:")
print(df_inconsistencias.head())

# Guardar en archivo
df_inconsistencias.to_csv("input/database/b7_datos.txt",sep='\t', index=False, encoding='latin1')

# # --- Conexión a BD ---
# conn = get_connection()
# cursor = conn.cursor()
# cursor.fast_executemany = True

# # --- Preparar ---
# records = df_inconsistencias.values.tolist()
# cols = ",".join(df_inconsistencias.columns)
# placeholders = ",".join(["?"] * len(df_inconsistencias.columns))

# sql = f"""
# INSERT INTO dbo.tb_detalle_inconsistencia_instrumento_foar_primaria_4to_agrupado_temp ({cols})
# VALUES ({placeholders})
# """

# # --- Inserción a BD ---
# batch_size = 1000
# for i in range(0, len(records), batch_size):
#     batch = records[i:i+batch_size]
#     try:
#         cursor.executemany(sql, batch)
#     except Exception as e:
#         print(f"Error en batch {i}-{i+batch_size}")
#         print("Ejemplo de registro problemático:", batch[0])
#         raise e

# # --- Confirmar ---
# conn.commit()
# cursor.close()
# conn.close()

# print("Inserción completada")