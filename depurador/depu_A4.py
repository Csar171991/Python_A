import pandas as pd
from conexion_bd import get_connection
import re
from path import DATABASE_PATH

database = DATABASE_PATH

# --- Lista ---
registros = []

# --- Archivo ---
# 188 primeras columnas
df_188 = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).iloc[:, :188]

# Seleccionar columnas faltantes
df_restante = df_188.join(
    pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str).iloc[:, 188:]
)

# --- Regex ---
regex_corto = re.compile(r"^[A-ZÁÉÍÓÚÑÜ\s'\-]{3,11}$") 
regex_largo = re.compile(r"^[A-ZÁÉÍÓÚÑÜ\s'\-]{3,22}$")  

# --- Campos ---
campos_cortos = ["paterno", "materno", "nombre1", "nombre2"]
campos_largos = [
    "nom_director", "ape_director",
    "nom_docente", "ape_docente",
    "nom_aplicador", "ape_aplicador",
    "nom_asistente", "ape_asistente",
    "nom_autoridad", "ape_autoridad"
]

# --- Validación ---
def validar_campos(campos, regex):
    for campo in campos:
        if campo in df_restante.columns:
            # Normalizar
            df_restante[campo] = df_restante[campo].fillna("").astype(str).str.strip()

            # Detectar inconsistencias
            invalidos = df_restante[
                (~df_restante[campo].apply(lambda x: bool(regex.match(x))))
                & (df_restante[campo] != "")
            ]

            # Guardar inconsistencias
            for idx, fila in invalidos.iterrows():
                registros.append({
                    "id_inconsistencia": 6,
                    "cod_barra": fila["cod_barra"],
                    "columna": campo,
                    "valor": fila[campo],
                    "nuevo_valor": "",
                    "estado_revision": 0,
                    "departamento": fila["departamento"],
                    "compuesta": 0,
                    "compuesta_values": "",
                    "id_usuario": " "
                })

# Ejecutar validación 
validar_campos(campos_cortos, regex_corto)
validar_campos(campos_largos, regex_largo)

# --- DataFrame ---
nombres_inconsistentes = pd.DataFrame(registros)

print("Filas inconsistentes en nombres:")
print(nombres_inconsistentes.head())

# --- Exportar ---
nombres_inconsistentes.to_csv("input/database/A4_datos.txt",sep='\t',index=False,encoding='latin1')

