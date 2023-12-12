#AUTO RECHAZOS

#En auto-rechazos, se actualiza la variable de Patrimonio (1m€) tanto si en la solicitud hay una persona o dos.
#Si hay uno de los usuarios dentro de una solicitud con discapacidad 3 o 4, se rechaza TODA la solicitud,
# es decir, salen del proceso todos esos usuarios asociados a esa solicitud.

print ('\n------------------------------------------------')
print ('Paso 2')
print ('------------------------------------------------ \n')


import psycopg2
from psycopg2 import sql
import pandas as pd
import time

#Delay de 15 segundos
time.sleep(20)

# Credenciales de la base de datos
dbname = "DBInmerso"
user = "postgres"
password = "postgres"
host = "postgres"  # Cambia esto según tu configuración
port = "5432"

# Crear la cadena de conexión
connection_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"

# Intentar establecer la conexión
try:
    with psycopg2.connect(connection_string) as connection:
        # Consulta para concatenar las tablas
        query_usuarios_discapacidad = """
                            SELECT
                                u.dni,
                                u.nombre,
                                u.apellido,
                                u.edad,
                                u.fecha_de_nacimiento,
                                u.id_solicitud,
                                d.grado_dis,
                                p.valoracion_patrimonio
                            FROM
                                usuarios AS u
                            JOIN discapacidad AS d ON u.dni = d.dni
                            JOIN patrimonio AS p ON u.dni = p.dni
                            """
    
                        
        # Crear un DataFrame con los resultados de la consulta concatenada
        df_discapacidad= pd.read_sql_query(query_usuarios_discapacidad, connection)
        


except psycopg2.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

df_discapacidad3=df_discapacidad[df_discapacidad['grado_dis']==3]
df_discapacidad4=df_discapacidad[df_discapacidad['grado_dis']==4]
df_patrimonio=df_discapacidad[df_discapacidad['valoracion_patrimonio']==2000000]
print (f"Usuarios con grado de discapacidad 3: {df_discapacidad3['dni'].nunique()}" )
print (f"Usuarios con grado de discapacidad 4: {df_discapacidad4['dni'].nunique()}" )
print (f"Usuarios con patrimonio superior a 1M: {df_patrimonio['dni'].nunique()}")


df_discapacidad = df_discapacidad[(df_discapacidad['grado_dis']==3) | (df_discapacidad['grado_dis']==4) | (df_discapacidad['valoracion_patrimonio']>1000000)]

df_discapacidad=df_discapacidad[['dni','nombre','apellido','edad','fecha_de_nacimiento','id_solicitud']]


#############################################################################################################################
######################################     Insertamos en tabla autorechazos  ################################################
#############################################################################################################################


try:
    with psycopg2.connect(connection_string) as connection:
        # Obtener el cursor
        cursor = connection.cursor()

        # Eliminar todos los datos de la tabla scoring
        delete_query = sql.SQL("DELETE FROM autorechazos")
        cursor.execute(delete_query)

        # Crear una consulta de inserción
        insert_query = sql.SQL("INSERT INTO autorechazos ({}) VALUES ({})").format(
            sql.SQL(', ').join(map(sql.Identifier, df_discapacidad.columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(df_discapacidad.columns))
        )

        # Insertar filas del DataFrame en la tabla de la base de datos
        for _, row in df_discapacidad.iterrows():
            cursor.execute(insert_query, tuple(row))

        # Confirmar la transacción
        connection.commit()

        print("Datos insertados correctamente en la tabla scoring.")

except psycopg2.Error as e:
    print(f"Error al insertar o borrar datos en la tabla scoring: {e}")








#############################################################################################################################
##################################################     borramos    ########################################################
#############################################################################################################################


try:
    with psycopg2.connect(connection_string) as connection:
        # Obtener el cursor
        cursor = connection.cursor()

        # Eliminar todos los datos de los usurios que han sido autorechazados
        
    
        delete_query_histo = sql.SQL(" delete FROM historial_usuarios WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos)) ")
        cursor.execute(delete_query_histo)  
        delete_query_fam = sql.SQL(" delete FROM familia_numerosa WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos)) ")
        cursor.execute(delete_query_fam) 
        delete_query_renta = sql.SQL(" delete FROM renta WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos))")
        cursor.execute(delete_query_renta)
        delete_query_patrimonio = sql.SQL(" delete FROM patrimonio WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos))")
        cursor.execute(delete_query_patrimonio)
        delete_query_discapacidad = sql.SQL(" delete FROM discapacidad WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos))")
        cursor.execute(delete_query_discapacidad)
        delete_query_disca= sql.SQL(" delete from patrimonio WHERE dni IN (select dni from usuarios where id_solicitud IN (select id_solicitud from autorechazos))")
        cursor.execute(delete_query_disca)
        delete_query_usuarios = sql.SQL(" DELETE FROM usuarios where id_solicitud IN (select id_solicitud from autorechazos) ")
        cursor.execute(delete_query_usuarios)
        delete_query_solicitudes = sql.SQL(" DELETE FROM solicitudes WHERE id_solicitud in (SELECT id_solicitud from autorechazos) ")
        cursor.execute(delete_query_solicitudes)
        delete_query_solicitudes = sql.SQL(" DELETE FROM scoring WHERE id_solicitud in (SELECT id_solicitud from autorechazos) ")
        cursor.execute(delete_query_solicitudes)
        
      

        # Confirmar la transacción
        connection.commit()

        print("Datos eliminados aplicando los autorechazos para discapacidad y patrimonio \n")

except psycopg2.Error as e:
    print(f"Error al editar la tabla solicitudes autorechazos: {e}")






#############################################################################################################################
##################################################     Actualizar    ########################################################
#############################################################################################################################




# actualizar
try:
    with psycopg2.connect(connection_string) as connection:
        # Consulta para concatenar las tablas
        query_actualizado = """
                            SELECT
                                u.dni,
                                u.nombre,
                                u.apellido,
                                u.edad,
                                u.fecha_de_nacimiento,
                                u.id_solicitud
                            FROM
                                usuarios AS u
                            """
                                

        # Crear un DataFrame con los resultados de la consulta concatenada
        df_actualizado = pd.read_sql_query(query_actualizado, connection)


except psycopg2.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

print (f" id solicitud unica tras autorechazo: {df_actualizado['id_solicitud'].nunique()}" )



print ('\n------------------------------------------------')
print ('Paso 3')
print ('------------------------------------------------ \n')

