import pandas as pd
from path import DATABASE_PATH
import re

# --- Leer archivo ---
df_188 = pd.read_csv(DATABASE_PATH, delimiter='\t', encoding='latin1', dtype=str).iloc[:, :188]

regex_dni = re.compile(r"^\d{8}$")  

# --- Función para validar duplicados e inválidos ---
def validar_dnis(df, columna):
    # Si la columna no existe, retornar vacío
    if columna not in df.columns:
        return pd.DataFrame(columns=[
            "id_inconsistencia","cod_barra","columna","valor","nuevo_valor",
            "estado_revision","departamento","compuesta","compuesta_values","id_usuario"
        ])

    # Normalización
    df[columna] = df[columna].fillna("").astype(str).str.strip()

    # Excluir vacíos y '99999999'
    df_validos = df[~df[columna].isin(["", "99999999"])]

    registros = []

    # ---  DNIs inválido ---
    df_invalidos = df_validos[~df_validos[columna].apply(lambda x: bool(regex_dni.match(x)))]
    for _, fila in df_invalidos.iterrows():
        registros.append({
            "id_inconsistencia": 4,   # siempre 4
            "cod_barra": fila.get("cod_barra", ""),
            "columna": columna,
            "valor": fila[columna],
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila.get("departamento", ""),
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

    # --- DNIs válidos pero duplicados ---
    df_validos_ok = df_validos[df_validos[columna].apply(lambda x: bool(regex_dni.match(x)))]
    df_duplicados = df_validos_ok[df_validos_ok.duplicated(columna, keep=False)]

    for _, fila in df_duplicados.iterrows():
        registros.append({
            "id_inconsistencia": 4,
            "cod_barra": fila.get("cod_barra", ""),
            "columna": columna,
            "valor": fila[columna],
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila.get("departamento", ""),
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

    return pd.DataFrame(registros)


# --- Columnas a validar ---
columnas_a_validar = [
    "dni_director",
    "dni_aplicador",
    "dni_docente",
    "dni_asistente"
]

resultados = []

for col in columnas_a_validar:
    df_incons = validar_dnis(df_188.copy(), col)
    if not df_incons.empty:
        resultados.append(df_incons)

# --- Consolidar ---
if resultados:
    df_final = pd.concat(resultados, ignore_index=True)
    df_final.to_csv("D:/shared/input/BD_762_FOAR 4P Muestral_1p/A2_todos.txt", sep="\t", index=False, encoding="latin1")
    print("Validaciones completadas. Archivo generado: A2_todos.txt")
else:
    print("No se encontraron inconsistencias en las columnas validadas.")

