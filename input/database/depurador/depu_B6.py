import pandas as pd
import json
from conexion_bd import get_connection
from path import DATABASE_PATH

# --- Archivo ---
database = DATABASE_PATH
df = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)

