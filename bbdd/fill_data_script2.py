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
    edad = random.randint(55, 90)
    fecha_nacimiento = fake.date_of_birth(minimum_age=edad)
    id_solicitud = random.randint(1, 1500)
    usuario_solicitante = fake.boolean()
    oficio_especial = fake.boolean()

    data.append([dni, nombre, apellido, edad, fecha_nacimiento, id_solicitud, usuario_solicitante, oficio_especial])

# Crear un DataFrame con los datos
df = pd.DataFrame(data, columns=['dni', 'nombre', 'apellido', 'edad', 'fecha_de_nacimiento', 'id_solicitud', 'usuario_solicitante', 'oficio_especial'])


# Obtener los id_solicitud triplicados--> no se puede
df['conteo_id_solicitud'] = df.groupby('id_solicitud')['id_solicitud'].transform('count')

# Filtrar las filas que cumplen con la condición
filas_a_actualizar = df['conteo_id_solicitud'] > 2

# Obtener los índices de las filas que cumplen con la condición
indices_a_actualizar = df.index[filas_a_actualizar]

# Asignar valores aleatorios de 5 cifras a 'id_solicitud' para las filas seleccionadas
df.loc[indices_a_actualizar, 'id_solicitud'] = [random.randint(30001, 59999) for _ in indices_a_actualizar]
df = df.drop('conteo_id_solicitud', axis=1)
df_usuarios = df
#VALIDAMOS
df_usuarios['conteo'] = df_usuarios.groupby('id_solicitud')['id_solicitud'].transform('count')
filas_a_actualizar = df_usuarios[df_usuarios['conteo'] > 2]
df_usuarios = df_usuarios.drop('conteo', axis=1)


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
        sql.SQL(', ').join(map(sql.Identifier, df_usuarios.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_usuarios.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_usuarios.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla usuarios.")

except Exception as e:
    print ("------------------------------")
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
    ingresos=random.randint(300, 2500)
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
    print ("------------------------------")
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

discapacidad = pd.DataFrame(data_discapacidad, columns=['grado_dis'])
df_dni=df['dni']
# Concatenar los DataFrames a lo largo de las columnas
df_discapacidad= pd.concat([df_dni, discapacidad], axis=1)



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
    print ("------------------------------")
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
    valor =random.randint(20000, 400000)
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
    print ("------------------------------")
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
    print ("------------------------------")
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
    resultado_solicitud_t_1 = random.randint(0, 3)
    viajes_t_1 = random.randint(0, 2)
    resultado_solicitud_t_2 = random.randint(0, 3)
    viajes_t_2 =random.randint(0, 2)
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
    print ("------------------------------")
    print(f"Error: {e}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()



##################################################################################
############################# DISPONIBILIDAD #####################################
##################################################################################

data_disponibilidad = []
start_date = datetime(2024, 1, 1)

#
for _ in range(2000):
    ciudades = fake.city()
    fecha = datetime(2024, random.randint(1,12), random.randint(1,29))
    num_hab_disp = random.randint(1,10)

    data_disponibilidad.append([ciudades,fecha, num_hab_disp])

df_disponibilidad = pd.DataFrame(data_disponibilidad, columns=['id_hotel','fecha_disponibilidad_hab','num_hab_disp'])

# Contar cuántas filas tienen los mismos valores en id_hotel y fecha_disponibilidad_hab
conteo_filas = df_disponibilidad.groupby(['id_hotel', 'fecha_disponibilidad_hab']).size().reset_index(name='conteo')
# Fusionar el DataFrame original con el DataFrame de conteo
df_disponibilidad_con_conteo = pd.merge(df_disponibilidad, conteo_filas, on=['id_hotel', 'fecha_disponibilidad_hab'], how='left')
df_disponibilidad_con_conteo[df_disponibilidad_con_conteo['conteo']>1].sort_values(by='id_hotel')
#filtramos
df_disponibilidad = df_disponibilidad_con_conteo[df_disponibilidad_con_conteo['conteo'] <= 1].sort_values(by='id_hotel')
df_disponibilidad.drop(columns='conteo', inplace=True)
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
    print ("------------------------------")
    print(f"Error: {e} disponibilidad")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()

      


##################################################################################
############################# SOLICITUDES ########################################
##################################################################################


#cogemos solo los valores unicos de la tabla usuarios
valores_unicos = df['id_solicitud'].unique().tolist()
df_solicitud = pd.DataFrame(valores_unicos, columns=['id_solicitud'])


# esta tabla tendrá el mismo tamañano que la de solicitudes

num_solicitud=len(df_solicitud)
data_solicitudes = []

# Generar 1000 registros
for _ in range(num_solicitud):
   anyo_solicitud = 2024
   usuarios_sol = 0
   renta_sol = 0

   data_solicitudes.append([anyo_solicitud,usuarios_sol,renta_sol])

solicitudes = pd.DataFrame(data_solicitudes, columns=['anyo_solicitud','usuarios_sol','renta_sol'])

# Concatenar los DataFrames con los ID solicitud de la tabla usuarios
df_solicitudes = pd.concat([df_solicitud, solicitudes], axis=1)
df_solicitudes



###########################################################################################################################
################################# Campos calculados en la tabla solicitudes ###############################################
###########################################################################################################################



# para la columna usuarios solicitud necesitamos calcular cuantos usuarios tienen esa solicitud
df_usuarios['conteo'] = df_usuarios.groupby('id_solicitud')['id_solicitud'].transform('count')
col= ['id_solicitud','conteo']
df_usuarios_merge1 = df_usuarios[col]
df_usuarios_merge1 = df_usuarios_merge1.drop_duplicates()
#merges
df_solicitudes_final_1 = pd.merge(df_solicitudes, df_usuarios_merge1, on='id_solicitud', how='left')
df_solicitudes_final_1['usuarios_sol'] = df_solicitudes_final_1['conteo']
df_solicitudes_final_1= df_solicitudes_final_1.drop('conteo', axis=1)




# para saber la renta conjunto de esa solicitud tenemos que hacer algo parecido, agrupar la solicitud por renta
df_renta_merge_usuarios = pd.merge(df_usuarios, df_renta, on='dni', how='left')
df_renta_merge_usuarios['ingresos_solicitud'] = df_renta_merge_usuarios.groupby('id_solicitud')['ingresos'].transform('sum')
col= ['id_solicitud','ingresos_solicitud']
df_usuarios_merge_ingresos = df_renta_merge_usuarios[col]
df_usuarios_merge_ingresos = df_usuarios_merge_ingresos.drop_duplicates()
#merge
df_solicitudes_final_2 = pd.merge(df_solicitudes_final_1, df_usuarios_merge_ingresos, on='id_solicitud', how='left')
df_solicitudes_final_2['renta_sol'] = df_solicitudes_final_2['ingresos_solicitud']
df_solicitudes_final_2= df_solicitudes_final_2.drop('ingresos_solicitud', axis=1)
df_solicitudes_final_2



# para saber la patrimonio conjunto de esa solicitud tenemos que hacer algo parecido, agrupar la solicitud por patrimonio
df_renta_merge_usuarios_patrimonio = pd.merge(df_usuarios,df_patrimonio [['dni','valoracion_patrimonio']], on='dni', how='left')
df_renta_merge_usuarios_patrimonio['patrimonio_solicitud'] = df_renta_merge_usuarios_patrimonio.groupby('id_solicitud')['valoracion_patrimonio'].transform('sum')
df_renta_merge_usuarios_patrimonio = df_renta_merge_usuarios_patrimonio[['id_solicitud','patrimonio_solicitud']]
df_renta_merge_usuarios_patrimonio = df_renta_merge_usuarios_patrimonio.drop_duplicates()
#merge
df_solicitudes_final_3 = pd.merge(df_solicitudes_final_2, df_renta_merge_usuarios_patrimonio, on='id_solicitud', how='left')
df_solicitudes_final_3['patrimonio_sol'] = df_solicitudes_final_3['patrimonio_solicitud']
df_solicitudes_final_3= df_solicitudes_final_3.drop('patrimonio_solicitud', axis=1)
df_solicitudes_final_3





#Añadimos: cogemos la tabla de disponibilidad para que coincida

df_disponibilidad_col =df_disponibilidad[['id_hotel','fecha_disponibilidad_hab']].sort_values(by='id_hotel')
df_solicitudes1 = pd.concat([df_solicitudes, df_disponibilidad_col], axis=1)
df_solicitudes1.rename(columns={'id_hotel': 'primera_opcion','fecha_disponibilidad_hab': 'fecha_1op'}, inplace=True)

df_desordenado = df_solicitudes1.sample(frac=1).reset_index(drop=True)
df_solicitudes2 = pd.concat([df_desordenado, df_disponibilidad_col], axis=1)
df_solicitudes2.rename(columns={'id_hotel': 'segunda_opcion','fecha_disponibilidad_hab': 'fecha_2op'}, inplace=True)

df_desordenado2 = df_solicitudes2.sample(frac=1).reset_index(drop=True)
df_solicitudes_final = pd.concat([df_desordenado2, df_disponibilidad_col], axis=1)
df_solicitudes_final.rename(columns={'id_hotel': 'tercera_opcion','fecha_disponibilidad_hab': 'fecha_3op'}, inplace=True)

df_solicitudes_final = df_solicitudes_final.dropna()
df_solicitudes_final

###################################################################################################################



# Crear una conexión para insertar los datos en la tabla patrimonio

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

try:
    # Crear una consulta de inserción
    insert_query = sql.SQL("INSERT INTO solicitudes ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, df_solicitudes_final_3.columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(df_solicitudes_final_3.columns))
    )

    # Obtener el cursor
    cursor = conn.cursor()

    # Insertar filas del DataFrame en la tabla de la base de datos
    for _, row in df_solicitudes_final_3.iterrows():
        cursor.execute(insert_query, tuple(row))

    # Confirmar la transacción
    conn.commit()

    print("Datos insertados correctamente en la tabla solicitudes.")

except Exception as e:
    print ("------------------------------")
    print(f"Error: {e} en solicitudes")
    print(f"Row causing the error: {row}")

finally:
    # Cerrar la conexión
    if conn is not None:
        conn.close()







print ('------------------------------------------------')
print ('Se han insertado todas las tablas correctamente')
print ('------------------------------------------------')




print ('------------------------------------------------')
print ('Paso 2')
print ('------------------------------------------------')
