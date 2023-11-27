import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql  
import time

# Retrasa la ejecucion del script 10 segundos, dando tiempo a que se levante la BBDD (no borrar!)
time.sleep(10)

# Configuración de Faker
fake = Faker('es_ES')

# Configuración del generador de números aleatorios
random.seed(42)

# Crear una lista para almacenar los datos
data = []

# Generar 1000 registros
for _ in range(1000):
    dni = random.randint(10000000, 99999999)  # Generar DNI de 8 dígitos
    nombre = fake.first_name()
    apellido = fake.last_name()
    edad = random.randint(70, 90)
    fecha_nacimiento = fake.date_of_birth(minimum_age=edad)
    id_solicitud = random.randint(1, 1000)
    usuario_solicitante = fake.boolean()
    oficio_especial = fake.boolean()

    data.append([dni, nombre, apellido, edad, fecha_nacimiento, id_solicitud, usuario_solicitante, oficio_especial])

# Crear un DataFrame con los datos
df = pd.DataFrame(data, columns=['dni', 'nombre', 'apellido', 'edad', 'fecha_de_nacimiento', 'id_solicitud', 'usuario_solicitante', 'oficio_especial'])


#Coonexion a BBDD............................................................................

dbname = "DBInmerso"
user = "postgres"
password = "postgres"
host = "postgres"
port = "5432"

# Crear una conexión
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:

    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO usuarios ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()