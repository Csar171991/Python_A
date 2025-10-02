import pandas as pd
import re
from path import DATABASE_PATH

# --- Lista ---
registros = []

# --- Archivo ---
database = DATABASE_PATH

# Cargar
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

# Lista de DNI a validar
dni_fields = [
    "dni_director",
    "dni_docente",
    "dni_aplicador",
    "dni_asistente",
    "dni_autoridad",
    "dni_est"
]

# Normalizar: convertir a string, quitar espacios
for col in dni_fields:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()

# Función
def validar_dni(valor):
    """
    Devuelve True si el DNI es válido (8 dígitos, no más de 4 nueves)
    Excepción: '99999999' se acepta siempre.
    """
    if valor == "99999999":
        return True
    if not re.fullmatch(r"\d{8}", valor):
        return False
    if valor.count("9") > 4:
        return False
    return True

# Revisar inconsistencias por campo
for col in dni_fields:
    if col in df.columns:
        for idx, fila in df.iterrows():
            valor = fila[col]

            # Condiciones de inconsistencia
            if valor == "" or not validar_dni(valor):
                registros.append({
                    "id_inconsistencia": 18, 
                    "cod_barra": fila.get("cod_barra", ""), 
                    "columna": col,
                    "valor": valor,
                    "nuevo_valor": "",
                    "estado_revision": 0,
                    "departamento": fila.get("departamento", ""),
                    "compuesta": 0,
                    "compuesta_values": "",
                    "id_usuario": " "
                })

# DataFrame
dni_inconsistentes = pd.DataFrame(registros)

print("Inconsistencias en DNIs (vacíos, inválidos o con más de 4 '9'):")
print(dni_inconsistentes.head())

# Guardar 
dni_inconsistentes.to_csv("input/database/A1_datos.txt",sep="\t",index=False,encoding="latin1")
