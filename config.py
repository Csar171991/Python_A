import os
from dotenv import load_dotenv

# Cargar archivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

print("BASE_DIR:", os.getenv("BASE_DIR"))
print("SHARED_DIR:", os.getenv("SHARED_DIR"))
print("PADRON_FILE:", os.getenv("PADRON_FILE"))
print("OUTPUT_DIR:", os.getenv("OUTPUT_DIR"))

def get_env_var(name: str, default: str = None, required: bool = True):
    """
    Obtiene una variable del entorno con validación.
    Si es requerida y no existe → lanza error.
    Si no es requerida → devuelve el default.
    """
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"❌ La variable {name} no está definida en el archivo .env")
    return value

# Base
BASE_DIR = get_env_var("BASE_DIR")
SHARED_DIR = get_env_var("SHARED_DIR", required=False)

# Archivos input
INVENTARIO_FILE = get_env_var("INVENTARIO_FILE")
PADRON_FILE = get_env_var("PADRON_FILE")

# Directorios output
OUTPUT_DIR = get_env_var("OUTPUT_DIR")
ERROR_VALIDACION_DIR = get_env_var("ERROR_VALIDACION_DIR")
MUESTRA_DIR = get_env_var("MUESTRA_DIR")
REPORT_DIR = get_env_var("REPORT_DIR")
DB_INGESTA = get_env_var("DB_INGESTA")

# Archivos output
DUPLICATES_VALIDACION_FILE = get_env_var("DUPLICATES_VALIDACION_FILE", required=False)
ERROR_VALIDACION_SALIDA_FILE = get_env_var("ERROR_VALIDACION_SALIDA_FILE", required=False)

