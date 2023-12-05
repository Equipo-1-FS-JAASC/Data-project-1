import psycopg2
import pandas as pd

# Credenciales de la base de datos
dbname = "DBInmerso"
user = "postgres"
password = "postgres"
host = "localhost"  # Cambia esto según tu configuración
port = "5432"

# Crear la cadena de conexión
connection_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"



# Intentar establecer la conexión
try:
    with psycopg2.connect(connection_string) as connection:
        # Consulta para concatenar las tablas
        query_concatenar = """
                            SELECT
                                *
                            FROM
                                usuarios 
                        """

        # Crear un DataFrame con los resultados de la consulta concatenada
        df_2 = pd.read_sql_query(query_concatenar, connection)

except psycopg2.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

print(f"Se ha hecho bien")
print ("Se aplican reglas de data governance")
print(df_2)

#>>>>>Ponderaciones<<<<<<#
edad_weight = 0.125
discapacidad_weight = 0.125
familia_weight = 0.125
renta_wheigt = 0.125
coche_wight = 0.125
oficio_especial_wight = 0.125
arrendador_wight = 0.125
participacion_previa_weight = 0.125

