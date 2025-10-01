import pandas as pd
from conexion_bd import get_connection
import re
from path import DATABASE_PATH

database = DATABASE_PATH

# --- Archivo ---
df_188 = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).iloc[:, :188]

# --- Normalización  ---
telefono_cols = [
    "tel1_director", "tel2_director",
    "tel1_docente", "tel2_docente",
    "tel1_aplicador", "tel2_aplicador",
    "tel1_asistente", "tel2_asistente",
    "tel1_autoridad", "tel2_autoridad"
]

for col in telefono_cols:
    if col in df_188.columns:
        df_188[col] = df_188[col].fillna("").astype(str).str.strip()

# --- Regex ---
regex_tel1 = re.compile(r"^\d{6}$|^\d{7}$")  
regex_tel2 = re.compile(r"^\d{9}$")          

# --- Validación ---
registros = []

# Definimos 
pares_telefonos = [
    ("tel1_director", "tel2_director"),
    ("tel1_docente", "tel2_docente"),
    ("tel1_aplicador", "tel2_aplicador"),
    ("tel1_asistente", "tel2_asistente"),
    ("tel1_autoridad", "tel2_autoridad")
]

for idx, fila in df_188.iterrows():
    for tel1, tel2 in pares_telefonos:
        # Validar tel1
        if fila[tel1] and not regex_tel1.match(fila[tel1]):
            registros.append({
                "id_inconsistencia": 5,
                "cod_barra": fila["cod_barra"],
                "columna": tel1,
                "valor": fila[tel1],
                "nuevo_valor": "",
                "estado_revision": 0,
                "departamento": fila["departamento"],
                "compuesta": 0,
                "compuesta_values": "",
                "id_usuario": " "
            })
        # Validar tel2
        if fila[tel2] and not regex_tel2.match(fila[tel2]):
            registros.append({
                "id_inconsistencia": 5,
                "cod_barra": fila["cod_barra"],
                "columna": tel2,
                "valor": fila[tel2],
                "nuevo_valor": "",
                "estado_revision": 0,
                "departamento": fila["departamento"],
                "compuesta": 0,
                "compuesta_values": "",
                "id_usuario": " "
            })

# --- DataFrame de inconsistencias ---
tel_inconsistentes = pd.DataFrame(registros)

print("Filas inconsistentes en teléfonos:")
print(tel_inconsistentes.head())

# --- Exportar a archivo ---
tel_inconsistentes.to_csv("D:/shared/input/BD_762_FOAR 4P Muestral_1p/A3_datos.txt",sep='\t',index=False,encoding='latin1')
