import pyodbc

print("Drivers disponíveis:")
print(pyodbc.drivers())

print("\nTentando conectar...")

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:srv00dital,1433;"
    "DATABASE=dital;"
    "UID=Dital;"
    "PWD=Dital@156197;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)

print("Conexão realizada com sucesso!")

cursor = conn.cursor()
cursor.execute("SELECT 1")

print("Resultado:", cursor.fetchone())

conn.close()