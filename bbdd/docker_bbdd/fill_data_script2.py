import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql  
import time
import numpy as np 

# Retrasa la ejecucion del script 10 segundos, dando tiempo a que se levante la BBDD (no borrar!)
time.sleep(10)

# Configuración de Faker
fake = Faker('es_ES')

# Configuración del generador de números aleatorios
random.seed(42)

##################################################################################
##################################### USUARIOS ###################################
##################################################################################

# Crear una lista para almacenar los datos
data = []

# Generar 1000 registros
for _ in range(1500):
    dni = random.randint(10000000, 99999999)  # Generar DNI de 8 dígitos
    nombre = fake.first_name()
    apellido = fake.last_name()
    edad = random.randint(70, 90)
    fecha_nacimiento = fake.date_of_birth(minimum_age=edad)
    id_solicitud = random.randint(1, 1500)
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

    print("Datos insertados correctamente en la tabla usuarios.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()





##################################################################################
############################# RENTA ##############################################
##################################################################################

# Crear una lista para almacenar los datos de la renta
data_renta = []
# Generar 1000 registros
for _ in range(1500):
    ingresos=random.randint(300, 5000)
    data_renta.append([ingresos])

renta = pd.DataFrame(data_renta, columns=['ingresos'])
df_dni=df['dni']

# Concatenar los DataFrames a lo largo de las columnas
df_renta= pd.concat([df_dni, renta], axis=1)
df_renta.head(10)


# Crear una conexión para insertar los datos en la tabla renta
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO renta ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_renta.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_renta.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_renta.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla renta.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()



##################################################################################
############################# DISCACIDAD #########################################
##################################################################################

# Crear una lista para almacenar los datos
data_discapacidad = []
# Generar 1000 registros
for _ in range(1500):
    #grado=random.randint(0,4)
    grado = random.choices([0, 1, 2, 3, 4], weights=[0.2, 0.2, 0.1, 0.002, 0.001])[0]
    data_discapacidad.append([grado])

discapcidad = pd.DataFrame(data_discapacidad, columns=['grado_dis'])
df_dni=df['dni']
# Concatenar los DataFrames a lo largo de las columnas
df_discapacidad= pd.concat([df_dni, discapcidad], axis=1)



# Crear una conexión para insertar los datos en la tabla discapcidad

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO discapacidad ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_discapacidad.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_discapacidad.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_discapacidad.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla discapacidad.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()

##################################################################################
############################# PATRIMONIO #########################################
##################################################################################


# Crear una lista para almacenar los datos
data_patrimonio = []
# Generar 1000 registros
for _ in range(1500):
    #grado=random.randint(0,4)
    valor =random.randint(20000, 500000)
    coche = fake.boolean()
    arrendador = fake.boolean()
    data_patrimonio.append([valor,coche,arrendador])

patrimonio = pd.DataFrame(data_patrimonio, columns=['valoracion_patrimonio','coche','arrendador'])
df_dni=df['dni']
# Concatenar los DataFrames a lo largo de las columnas
df_patrimonio= pd.concat([df_dni, patrimonio], axis=1)
df_patrimonio.loc[df_patrimonio['dni'] == 95822412, 'valoracion_patrimonio'] = 2000000




# Crear una conexión para insertar los datos en la tabla patrimonio

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO patrimonio ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_patrimonio.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_patrimonio.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_patrimonio.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla patrimonio.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()


##################################################################################
############################# FAMILIA_NUMEROSA ##################################
##################################################################################

fam_numerosa = []
# Generar 1000 registros
for _ in range(1500):
    #Tipo_fam_num = random.choices([0, 1, 2], weights=[0.5, 0.35, 0.15])[0]
    Tipo_fam_num=random.randint(0, 2)
    fam_numerosa.append([Tipo_fam_num])

numerosa = pd.DataFrame(fam_numerosa, columns=['tipo_fam_num'])
df_dni=df['dni']
# Concatenar los DataFrames a lo largo de las columnas
df_familia_numerosa= pd.concat([df_dni, numerosa], axis=1)
df_familia_numerosa = df_familia_numerosa

# Crear una conexión para insertar los datos en la tabla patrimonio

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO familia_numerosa ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_familia_numerosa.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_familia_numerosa.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_familia_numerosa.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla familia numerosa.")

except Exception as e:
    print(f"Error: {e} en familia numerosa.")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()


##################################################################################
############################# HISTORIAL_USUARIOS #################################
##################################################################################

historial= []
# Generar 1000 registros
for _ in range(1500):
    cross_selling = fake.boolean(False)
    resultado_solicitud_t_1 = random.randint(70, 90)
    viajes_t_1 = random.randint(1, 2)
    resultado_solicitud_t_2 = random.randint(70, 90)
    viajes_t_2 =random.randint(1, 2)
    suma_viajes_t_1_y_t_2 = viajes_t_1 + viajes_t_2  
    scoring_ind_participacion_previa = random.randint(50, 90)

    historial.append([cross_selling,resultado_solicitud_t_1,viajes_t_1,resultado_solicitud_t_2,viajes_t_2,suma_viajes_t_1_y_t_2,scoring_ind_participacion_previa])

historial = pd.DataFrame(historial, columns=['cross_selling','resultado_solicitud_t_1','viajes_t_1','resultado_solicitud_t_2','viajes_t_2','suma_viajes_t_1_y_t_2','scoring_ind_participacion_previa'])
df_dni=df['dni']
# Concatenar los DataFrames a lo largo de las columnas
df_historial_usuario= pd.concat([df_dni, historial], axis=1)



# Crear una conexión para insertar los datos en la tabla patrimonio

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO historial_usuarios ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_historial_usuario.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_historial_usuario.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_historial_usuario.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla historial usuarios.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()



##################################################################################
############################# DISPONIBILIDAD #####################################
##################################################################################

#generamos la lista de ciudades falsas
lista_ciudades = [fake.city() for _ in range(1500)]
#creamos un dataframe a partir de estas ciudades falsas
df_lista_ciudades = pd.DataFrame(lista_ciudades, columns=['ciudades'])
valores_unicos_ciudades = df_lista_ciudades['ciudades'].unique().tolist()
df_valores_unicos_ciudades = pd.DataFrame(valores_unicos_ciudades, columns=['ciudades'])

num_ciudades=len (valores_unicos_ciudades)

data_disponibilidad = []
start_date = datetime(2024, 1, 1)

# Generar 1000 registros
for _ in range(num_ciudades):
    fecha = datetime(2024, random.randint(1,12), random.randint(1,29))
    num_hab_disp = random.randint(1,10)
    
    data_disponibilidad.append([fecha, num_hab_disp])

disponibilidad = pd.DataFrame(data_disponibilidad, columns=['fecha_disponibilidad_hab','num_hab_disp'])
# Concatenar los DataFrames a lo largo de las columnas
df_disponibilidad = pd.concat([df_valores_unicos_ciudades, disponibilidad], axis=1)
df_disponibilidad.rename(columns={'ciudades': 'id_hotel'}, inplace=True)
df_disponibilidad



# Crear una conexión para insertar los datos en la tabla patrimonio
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO disponibilidad ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_disponibilidad.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_disponibilidad.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_disponibilidad.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla disponibilidad.")

except Exception as e:
    print(f"Error: {e} disponibilidad")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()

      


##################################################################################
############################# SOLICITUDES ########################################
##################################################################################

#cogemos solo los valores uncios de la tabla usuarios
valores_unicos = df['id_solicitud'].unique().tolist()
df_solicitud = pd.DataFrame(valores_unicos, columns=['id_solicitud'])

num_solicitud=len(df_solicitud)
data_solicitudes = []

# Generar 1000 registros
for _ in range(num_solicitud):
   anyo_solicitud = 2024
   usuarios_sol = 'a'
   renta_sol = 0
   primera_opcion = random.choice(lista_ciudades)
   segunda_opcion = random.choice(lista_ciudades)
   tercera_opcion = random.choice(lista_ciudades)

   data_solicitudes.append([anyo_solicitud,usuarios_sol,renta_sol,primera_opcion,segunda_opcion,tercera_opcion])

solicitudes = pd.DataFrame(data_solicitudes, columns=['anyo_solicitud','usuarios_sol','renta_sol','primera_opcion','segunda_opcion','tercera_opcion'])
# Concatenar los DataFrames a lo largo de las columnas
df_solicitudes = pd.concat([df_solicitud, solicitudes], axis=1)

# Crear una conexión para insertar los datos en la tabla patrimonio

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO solicitudes ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_solicitudes.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_solicitudes.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_solicitudes.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla solicitudes.")

except Exception as e:
    print(f"Error: {e} en solicitudes")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()


