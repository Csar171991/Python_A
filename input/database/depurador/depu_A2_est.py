import pandas as pd
from conexion_bd import get_connection
import re
from path import DATABASE_PATH

# --- Lista  ---
registros = []

database = DATABASE_PATH

# Cargar toda la base
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

regex_dni = re.compile(r"^\d{8}$")  

# Normalizar campos
df["dni_est"] = df["dni_est"].fillna("").astype(str).str.strip()
df["asis_dia1_lec"] = df["asis_dia1_lec"].fillna("0").astype(str).str.strip()
df["asis_dia1_mat"] = df["asis_dia1_mat"].fillna("0").astype(str).str.strip()
df["asis_dia2_mat"] = df["asis_dia2_mat"].fillna("0").astype(str).str.strip()
df["asis_dia2_lec"] = df["asis_dia2_lec"].fillna("0").astype(str).str.strip()

# Filtrar solo estudiantes con asistencia en cualquiera de los campos
df_presentes = df[
    (df["asis_dia1_lec"] == "1") |
    (df["asis_dia1_mat"] == "1") |
    (df["asis_dia2_mat"] == "1") |
    (df["asis_dia2_lec"] == "1")
]

# Excluir DNIs vacíos y "99999999"
df_presentes = df_presentes[
    (df_presentes["dni_est"] != "") &
    (df_presentes["dni_est"] != "99999999")
]

# --- DNIs inválidos ---
for idx, fila in df_presentes.iterrows():
    valor = str(fila["dni_est"]).strip()
    # Si no cumple con 8 dígitos, registrar inconsistencia
    if not re.fullmatch(r"\d{8}", valor):
        registros.append({
            "id_inconsistencia": 1,
            "cod_barra": fila["cod_barra"],
            "columna": "dni_est",
            "valor": valor,
            "nuevo_valor": "",
            "estado_revision": 0,
            "departamento": fila["departamento"],
            "compuesta": 0,
            "compuesta_values": "",
            "id_usuario": " "
        })

# ---  DNIs válidos pero duplicados ---
df_validos_ok = df_presentes[df_presentes["dni_est"].apply(lambda x: bool(regex_dni.match(x)))]
duplicados = df_validos_ok[df_validos_ok.duplicated("dni_est", keep=False)]

for _, fila in duplicados.iterrows():
    registros.append({
        "id_inconsistencia": 1,
        "cod_barra": fila["cod_barra"],
        "columna": "dni_est",
        "valor": fila["dni_est"],
        "nuevo_valor": "",
        "estado_revision": 0,
        "departamento": fila["departamento"],
        "compuesta": 0,
        "compuesta_values": "",
        "id_usuario": " "
    })

# DataFrame final con inconsistencias
dni_inconsistentes = pd.DataFrame(registros)

print("Inconsistencias de DNI (inválidos o duplicados con asistencia):")
print(dni_inconsistentes.head())

# Guardar en archivo
dni_inconsistentes.to_csv("D:/shared/input/BD_762_FOAR 4P Muestral_1p/A2_dni_est1.txt",sep="\t",index=False,encoding="latin1")
