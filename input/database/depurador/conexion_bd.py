import pyodbc

def get_connection():
    """
    Retorna la conexión a SQL Server
    """
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=sql5106.site4now.net;"
        "DATABASE=db_aac49f_depuracioningesta2;"
        "UID=db_aac49f_depuracioningesta2_admin;"
        "PWD=Admin2025#"
    )
    return conn




