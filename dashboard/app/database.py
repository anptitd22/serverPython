import pyodbc

def get_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=datamart_db;'
        'UID=sa;'
        'PWD=an147258'
    )
    return conn
