import pandas as pd
import json
from path import DATABASE_PATH
from conexion_bd import get_connection

# --- Parámetros ---
database = DATABASE_PATH
inconsistencia = 23

# --- Cargar  ---
df = pd.read_csv(database, delimiter="\t", encoding="latin1", dtype=str).fillna("")

# --- Normalizar ---
for c in ["forma_dia1", "forma_dia2", "paterno", "materno", "nombre1", "nombre2"]:
    df[c] = df[c].astype(str).str.strip()

# --- Rangos válidos ---
validos_dia1 = {f"{i:02d}" for i in range(1, 7)}   # 01-06
validos_dia2 = {f"{i:02d}" for i in range(7, 13)}  # 07-12

# --- Inicializar lista de inconsistencias ---
registros = []

# --- Iterar filas ---
for _, fila in df.iterrows():
    # Verificar si hay nombres llenos
    if not any(fila[col] for col in ["paterno", "materno", "nombre1", "nombre2"]):
        continue

    dia1 = fila["forma_dia1"]
    dia2 = fila["forma_dia2"]

    # Validaciones: si está vacío o fuera del rango permitido
    invalido_dia1 = (dia1 == "") or (dia1 not in validos_dia1)
    invalido_dia2 = (dia2 == "") or (dia2 not in validos_dia2)

    if invalido_dia1 or invalido_dia2:
        columnas = ["forma_dia1", "forma_dia2"]
        valores = {
            "forma_dia1": dia1,
            "forma_dia2": dia2
        }

        registros.append({
            "id_inconsistencia": inconsistencia,
            "cod_barra": fila.get("cod_barra", ""),
            "columna": json.dumps({"columnas_forma": columnas}, ensure_ascii=False),
            "valor": json.dumps({"columnas_forma": valores}, ensure_ascii=False),
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila.get("departamento", ""),
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

# --- DataFrame final ---
df_inconsistencias = pd.DataFrame(registros)

# --- Mostrar ---
print("Inconsistencias detectadas en forma_dia1 y forma_dia2:")
print(df_inconsistencias.head())

# --- Exportar ---
df_inconsistencias.to_csv("input/database/D5_datos.txt", sep="\t", index=False, encoding="latin1")
