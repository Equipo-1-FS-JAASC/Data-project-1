#!/usr/bin/env python
# coding: utf-8

# ### >>>INGESTA DE DATOS

# In[134]:


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
        query_concatenar_1 = """
                            SELECT
                                u.dni,
                                u.nombre,
                                u.apellido,
                                u.edad,
                                u.fecha_de_nacimiento,
                                u.id_solicitud,
                                u.usuario_solicitante,
                                u.oficio_especial,
                                r.ingresos,
                                d.grado_dis,
                                fn.tipo_fam_num,
                                p.valoracion_patrimonio,
                                p.coche,
                                p.arrendador,
                                h.resultado_solicitud_t_1,
                                h.resultado_solicitud_t_2,
                                h.viajes_t_1,
                                h.viajes_t_2,
                                h.scoring_ind_participacion_previa,
                                h.suma_viajes_t_1_y_t_2
                            FROM
                                usuarios AS u
                            JOIN renta AS r ON u.dni = r.dni
                            JOIN discapacidad AS d ON u.dni = d.dni
                            JOIN familia_numerosa AS fn ON u.dni = fn.dni
                            JOIN patrimonio AS p ON u.dni = p.dni
                            JOIN historial_usuarios AS h ON u.dni = h.dni;
                            """
        query_concatenar_2 = """
                            SELECT 
                                u.dni, 
                                u.id_solicitud, 
                                s.*
                            FROM usuarios u
                            LEFT JOIN solicitudes s ON u.id_solicitud = s.id_solicitud;
                                """
                                


        # Crear un DataFrame con los resultados de la consulta concatenada
        df = pd.read_sql_query(query_concatenar_1, connection)
        df_2 = pd.read_sql_query(query_concatenar_2, connection)

except psycopg2.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

#>>>>>Ponderaciones<<<<<<#
edad_weight = 0.1
discapacidad_weight = 0.1
familia_weight = 0.1
renta_wheigt = 0.1
coche_wight = 0.1
oficio_especial_wight = 0.1
arrendador_wight = 0.1
participacion_previa_weight = 0.1
patrimonio_weight = 0.1
renta_weight = 0.1


# ### >>>EDAD.SCORE

# In[137]:


def puntaje_edad_nueva(edad):
    if edad < 60:
        return 1
    elif 60 <= edad < 70:
        return 20
    elif 70 <= edad < 80:
        return 30
    else:  # Edad >= 80
        return 40

def procesar_edad(df):
    # Copiar el DataFrame para evitar modificar el original
    df_edad = df.copy()

    # Aplicar la función
    df_edad['score_edad'] = df_edad['edad'].apply(puntaje_edad_nueva)

    # Calcular el Min-Max Scaling 
    min_valor = df_edad['score_edad'].min()
    max_valor = df_edad['score_edad'].max()
    rango_deseado = 100
    min_deseado = 0

    df_edad['score_edad_scaled'] = ((df_edad['score_edad'] - min_valor) / (max_valor - min_valor)) * rango_deseado + min_deseado

    # Multiplicar 'score_edad_scaled' por el ponderador
    df_edad['score_edad_weighted'] = df_edad['score_edad_scaled'] * edad_weight
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_edad[['dni', 'edad', 'score_edad', 'score_edad_scaled','score_edad_weighted']].sort_values(by='dni')

# aplicar funcion
df_edad = procesar_edad(df)


# ### >>>DISCAPACIDAD.SCORE

# In[140]:


def puntaje_discapacidad(grado_dis):
    if grado_dis == 0:
        return 0
    elif grado_dis == 1:
        return 10
    else:  # grado_dis == 2
        return 20

def procesar_discapacidad(df):
    # Copiar el DataFrame para evitar modificar el original
    df_discapacidad = df.copy()

    # Aplicar la función 
    df_discapacidad['score_discapacidad'] = df_discapacidad['grado_dis'].apply(puntaje_discapacidad)

    # Calcular el Min-Max Scaling 
    min_valor = df_discapacidad['score_discapacidad'].min()
    max_valor = df_discapacidad['score_discapacidad'].max()
    rango_deseado = 100
    min_deseado = 0

    df_discapacidad['score_discapacidad_scaled'] = ((df_discapacidad['score_discapacidad'] - min_valor) / (max_valor - min_valor)) * rango_deseado + min_deseado

    df_discapacidad['score_discapacidad_weighted'] = df_discapacidad['score_discapacidad_scaled'] * discapacidad_weight
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_discapacidad[['dni', 'score_discapacidad', 'score_discapacidad_scaled','score_discapacidad_weighted']].sort_values(by='dni')

# aplicar funcion
df_discapacidad = procesar_discapacidad(df)
df_discapacidad


# ### >>>SCORE FAMILIA NUMEROSA

# In[143]:


def puntaje_familia(tipo_fam_num):
    if tipo_fam_num == 0:
        return 0
    elif tipo_fam_num == 1:
        return 5
    else:  # tipo_fam_num == 2
        return 10

def procesar_familia(df):
    # Copiar el DataFrame para evitar modificar el original
    df_familia = df.copy()

    # Aplicar la función 
    df_familia['score_familia'] = df_familia['tipo_fam_num'].apply(puntaje_familia)

    # Calcular el Min-Max Scaling 
    min_valor = df_familia['score_familia'].min()
    max_valor = df_familia['score_familia'].max()
    rango_deseado = 100
    min_deseado = 0

    df_familia['score_familia_scaled'] = ((df_familia['score_familia'] - min_valor) / (max_valor - min_valor)) * rango_deseado + min_deseado

    df_familia['score_familia_weighted'] = df_familia['score_familia_scaled'] * familia_weight
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_familia[['dni', 'score_familia', 'score_familia_scaled','score_familia_weighted']].sort_values(by='dni')

# aplicar funcion
df_familia = procesar_familia(df)
df_familia


# ### >>>COCHE - ARRENDADOR - OFICIO ESPECIAL

# In[146]:


def puntaje_coche_arrendador(coche_arrendador_oficio):
    if coche_arrendador_oficio == True:
        return 0
    else:  #  == False
        return 100

def puntaje_oficio(coche_arrendador_oficio):
    if coche_arrendador_oficio == True:
        return 100
    else:  #  == False
        return 0


def procesar_coche_arrendador_oficio(df):
    # Copiar el DataFrame para evitar modificar el original
    df_coche_arrendador_oficio = df.copy()

    # Aplicar la función 
    df_coche_arrendador_oficio['score_coche'] = df_coche_arrendador_oficio['coche'].apply(puntaje_coche_arrendador)
    df_coche_arrendador_oficio['score_arrendador'] = df_coche_arrendador_oficio['arrendador'].apply(puntaje_coche_arrendador)
    df_coche_arrendador_oficio['score_oficio_especial'] = df_coche_arrendador_oficio['oficio_especial'].apply(puntaje_oficio)


    df_coche_arrendador_oficio['score_coche_weighted'] = df_coche_arrendador_oficio['score_coche'] * coche_wight
    df_coche_arrendador_oficio['score_arrendador_weighted'] = df_coche_arrendador_oficio['score_arrendador'] * arrendador_wight
    df_coche_arrendador_oficio['score_oficio_especial_weighted'] = df_coche_arrendador_oficio['score_oficio_especial'] * oficio_especial_wight
     
    # Devolver el DataFrame con las columnas seleccionadas
    return df_coche_arrendador_oficio[['dni', 'score_coche', 'score_coche_weighted','score_arrendador','score_arrendador_weighted','score_oficio_especial', 'score_oficio_especial_weighted','fecha_de_nacimiento','id_solicitud' ]].sort_values(by='dni')


# aplicar funcion
df_coche_arrendador_oficio = procesar_coche_arrendador_oficio(df)


# ### >>>PARTICIPACION PREVIA

# In[149]:


def participacion_previa(resultado_t_1, suma_viajes_t_1y_t_2, viajes_t_1, viajes_t_2):
    if resultado_t_1 == 2:
        return 175
    elif suma_viajes_t_1y_t_2 == 0:
        return 50
    elif viajes_t_1 == 0 and viajes_t_2 > 0:
        return 40
    elif viajes_t_1 > 0 and viajes_t_2 == 0:
        return 20
    elif suma_viajes_t_1y_t_2 >= 3:
            return 0
    else:
        return 10

def procesar_participacion_previa(df):
    # Copiar el DataFrame para evitar modificar el original
    df_participacion_previa = df.copy()

    # Aplicar la función
    df_participacion_previa['score_participacion_previa'] = df_participacion_previa.apply(
    lambda row: participacion_previa(
        row['resultado_solicitud_t_1'],
        row['suma_viajes_t_1_y_t_2'],
        row['viajes_t_1'],
        row['viajes_t_2']
    ),
    axis=1
    
    
    
)
    # Calcular el Min-Max Scaling 
    min_valor = df_participacion_previa['score_participacion_previa'].min()
    max_valor = df_participacion_previa['score_participacion_previa'].max()
    rango_deseado = 100
    min_deseado = 0

    df_participacion_previa['score_participacion_previa_scaled'] = (
        (df_participacion_previa['score_participacion_previa'] - min_valor) / (max_valor - min_valor)
    ) * rango_deseado + min_deseado

    df_participacion_previa['score_participacion_previa_weighted'] = (
        df_participacion_previa['score_participacion_previa_scaled'] * participacion_previa_weight
    )
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_participacion_previa[[
        'dni', 
        'score_participacion_previa', 
        'score_participacion_previa_scaled',
        'score_participacion_previa_weighted'
    ]].sort_values(by='score_participacion_previa_weighted')

# Aplicar la función
df_participacion_previa = procesar_participacion_previa(df)


# ### >>>PATRIMONIO

# In[153]:


def calcular_puntaje_patrimonio(patrimonio_sol):

    # Calcular puntaje basado en las condiciones dadas
    if patrimonio_sol <= 20000:
        return 150
    elif 20000 <= patrimonio_sol <= 40000:
        return 100
    elif 40000 <= patrimonio_sol <= 80000:
        return 55
    elif 80000 <= patrimonio_sol <= 130000:
        return 45
    elif 130000 <= patrimonio_sol <= 170000:
        return 35
    elif 170000 <= patrimonio_sol <= 230000:
        return 25
    elif 230000 <= patrimonio_sol <= 275000:
        return 15
    elif 275000 <= patrimonio_sol <= 325000:
        return 5
    else:
        return 0
    
    
def procesar_patrimonio(df):
    # Copiar el DataFrame para evitar modificar el original
    df_patrimonio = df_2.copy()

    # Calcular puntaje basado en las condiciones dadas
    df_patrimonio['score_patrimonio'] = df_patrimonio['patrimonio_sol'].apply(calcular_puntaje_patrimonio)

    # Calcular el Min-Max Scaling 
    min_valor = df_patrimonio['score_patrimonio'].min()
    max_valor = df_patrimonio['score_patrimonio'].max()
    rango_deseado = 100
    min_deseado = 0

    df_patrimonio['score_patrimonio_scaled'] = ((df_patrimonio['score_patrimonio'] - min_valor) / (max_valor - min_valor)) * rango_deseado + min_deseado

    df_patrimonio['score_patrimonio_weighted'] = df_patrimonio['score_patrimonio_scaled'] * familia_weight
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_patrimonio[['dni', 'score_patrimonio', 'score_patrimonio_scaled','score_patrimonio_weighted']].sort_values(by='dni')

# Aplicar función
df_patrimonio = procesar_patrimonio(df)


# ### >>>RENTA

# In[156]:


def calcular_puntaje_renta(renta_sol, usuarios_sol):
    if usuarios_sol == 2:
        renta_sol /= 1.33

    if renta_sol <= 484.61:
        return 50
    elif 484.61 < renta_sol <= 900:
        return 45
    elif 900 < renta_sol <= 1050:
        return 40
    elif 1050 < renta_sol <= 1200:
        return 35
    elif 1200 < renta_sol <= 1350:
        return 30
    elif 1350 < renta_sol <= 1500:
        return 25
    elif 1500 < renta_sol <= 1650:
        return 20
    elif 1650 < renta_sol <= 1800:
        return 15
    elif 1800 < renta_sol <= 1950:
        return 10
    elif 1950 < renta_sol <= 2100:
        return 5
    else:
        return 0
def procesar_renta(df):
    # Copiar el DataFrame para evitar modificar el original
    df_renta = df_2.copy()

    # Calcular puntaje basado en las condiciones dadas
    df_renta['score_renta'] = df_renta.apply(lambda row: calcular_puntaje_renta(row['renta_sol'], row['usuarios_sol']), axis=1)

    # Calcular el Min-Max Scaling
    min_valor = df_renta['score_renta'].min()
    max_valor = df_renta['score_renta'].max()
    rango_deseado = 100
    min_deseado = 0

    df_renta['score_renta_scaled'] = ((df_renta['score_renta'] - min_valor) / (max_valor - min_valor)) * rango_deseado + min_deseado

    df_renta['score_renta_weighted'] = df_renta['score_renta_scaled'] * familia_weight
    
    # Devolver el DataFrame con las columnas seleccionadas
    return df_renta[['dni', 'score_renta', 'score_renta_scaled', 'score_renta_weighted']].sort_values(by='dni')

# Aplicar función
df_renta = procesar_renta(df)


# ### >>>MERGE 

# In[159]:


#Merge de dataframes
score_merged = pd.merge(df_edad, df_discapacidad, on='dni', how='inner')\
               .merge(df_coche_arrendador_oficio, on='dni', how='inner')\
               .merge(df_participacion_previa, on='dni', how='inner')\
               .merge(df_patrimonio, on='dni', how='inner')\
               .merge(df_renta, on='dni', how='inner')\
               .merge(df_familia, on='dni', how='inner')


# seleccion de columnas que terminan en '_weighted'
columnas_weighted = [col for col in score_merged.columns if col.endswith('_weighted')]

# Crea la nueva columna 'score_total' sumando todas las columnas '_weighted'
score_merged['score_total'] = score_merged[columnas_weighted].sum(axis=1)
score_merged = score_merged.sort_values(by='score_total', ascending=False)



#Seleccion de variables weighted y otras
score_merged_weighted = score_merged.loc[:, [
                                            'id_solicitud' ,
                                            'score_total',
                                            'score_edad_weighted',
                                            'score_discapacidad_weighted',
                                            'score_coche_weighted',
                                            'score_arrendador_weighted',
                                            'score_oficio_especial_weighted',
                                            'score_familia_weighted',
                                            'score_patrimonio_weighted',
                                            'score_renta_weighted',
                                            'score_participacion_previa_weighted']]



score_merged_weighted_rename = {

    'id_solicitud': 'id_solicitud',
    'score_total': 'score',
    'score_edad_weighted': 'edad_score',
    'score_discapacidad_weighted': 'discapacidad_score',
    'score_coche_weighted': 'coche_score',
    'score_arrendador_weighted': 'arrendador_score',
    'score_oficio_especial_weighted': 'oficio_especial_score',
    'score_participacion_previa_weighted': 'part_previa_score',
    'score_patrimonio_weighted': 'patrimonio_score',
    'score_renta_weighted': 'renta_score',
    'score_familia_weighted': 'familia_score'
    
}

# Renombrar todas las columnas
score_merged_weighted = score_merged_weighted.rename(columns=score_merged_weighted_rename)

#Group by para agrupar usuarios en solicitudes. Se usa el promedio entre ambos usuarios
score_merged_weighted_group = score_merged_weighted.groupby('id_solicitud').mean().reset_index()

score_merged_weighted_group['index_'] = score_merged_weighted_group['score'].rank(ascending=False, method='min').astype(int)
score_merged_weighted_group_sorted = score_merged_weighted_group.sort_values(by='index_', ascending=True)
score_merged_weighted_group_sorted['index_'] = score_merged_weighted_group_sorted['index_'].astype(int)



# 

# ### >>>EVALUACION DEL MODELO

# ### >>>SUBIR TABLA SCORE

# In[163]:


# Intentar establecer la conexión
try:
    with psycopg2.connect(connection_string) as connection:
        # Obtener el cursor
        cursor = connection.cursor()

        # Eliminar todos los datos de la tabla scoring
        delete_query = sql.SQL("DELETE FROM scoring")
        cursor.execute(delete_query)

        # Crear una consulta de inserción
        insert_query = sql.SQL("INSERT INTO scoring ({}) VALUES ({})").format(
            sql.SQL(', ').join(map(sql.Identifier, score_merged_weighted_group_sorted.columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(score_merged_weighted_group_sorted.columns))
        )

        # Insertar filas del DataFrame en la tabla de la base de datos
        for _, row in score_merged_weighted_group_sorted.iterrows():
            cursor.execute(insert_query, tuple(row))

        # Confirmar la transacción
        connection.commit()

        print("\nDatos insertados correctamente en la tabla scoring. \n")

except psycopg2.Error as e:
    print(f"Error al insertar o borrar datos en la tabla scoring: {e}")


# ### >>> ASIGNACION DE PLAZAS

# In[164]:


#Temporizador
time.sleep(5)

# Crear la cadena de conexión
connection_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"

# Intentar establecer la conexión
try:
    with psycopg2.connect(connection_string) as connection:
        # Consulta para concatenar las tablas                        
        query_concatenar_3 = """
            SELECT 
                s.id_solicitud,
                sc.index_,
                sc.score, 
                s.primera_opcion,
                s.fecha_1op,
                s.segunda_opcion, 
                s.fecha_2op,
                s.tercera_opcion,
                s.fecha_3op
            FROM 
                solicitudes s
            JOIN scoring sc ON s.id_solicitud = sc.id_solicitud;
        """

        query_concatenar_5 = """
            SELECT 
                d.id_hotel,
                d.fecha_disponibilidad_hab,
                d.num_hab_disp,
                d.id_plaza,
                ho.localizacion
            FROM disponibilidad d
            JOIN hoteles ho ON d.id_hotel = ho.id_hotel;
        """

        # Crear DataFrames con los resultados de las consultas
        solicitudes = pd.read_sql_query(query_concatenar_3, connection)
        disponibilidad = pd.read_sql_query(query_concatenar_5, connection)

except psycopg2.Error as e:
    print("Error durante la conexión a la base de datos:", e)
finally:
    if connection:
        connection.close()


# In[165]:


#FUNCION DE ASIGNACION
# Función para encontrar una coincidencia entre las preferencias y la disponibilidad
def encontrar_coincidencia(preferencia, fecha_preferencia, disponibilidad, plazas_asignadas):
    filtro = (disponibilidad['fecha_disponibilidad_hab'] == fecha_preferencia) & \
             (disponibilidad['localizacion'] == preferencia)
    coincidencias = disponibilidad[filtro]
    
    # Filtrar solo las plazas que aún no han sido asignadas
    coincidencias = coincidencias[~coincidencias['id_plaza'].isin(plazas_asignadas)]
    
    if not coincidencias.empty:
        id_plaza_asignada = coincidencias.iloc[0]['id_plaza']
        
        # Marcar la plaza como asignada en el DataFrame de disponibilidad
        disponibilidad.loc[disponibilidad['id_plaza'] == id_plaza_asignada, 'asignada'] = True
        
        return id_plaza_asignada
    else:
        return None


disponibilidad['asignada'] = False # Agregar columna 'asignada' en disponibilidad y se inicializa como False
solicitudes['id_plaza'] = None  # Inicializar la columna
solicitudes = solicitudes.sort_values(by='index_') # Ordena df por index 
plazas_asignadas = set()  # Lista para rastrear plazas ya asignadas


#Itera en cada fila aplicando la funcion y respetando el orden del indice
for idx, row in solicitudes.iterrows():
    id_solicitud = row['id_solicitud']
    id_plaza_asignada = encontrar_coincidencia(row['primera_opcion'], row['fecha_1op'], disponibilidad, plazas_asignadas) or \
                        encontrar_coincidencia(row['segunda_opcion'], row['fecha_2op'], disponibilidad, plazas_asignadas) or \
                        encontrar_coincidencia(row['tercera_opcion'], row['fecha_3op'], disponibilidad, plazas_asignadas)

    # Verificar si la plaza ya está asignada o supera la cantidad máxima
    if id_plaza_asignada is not None:
        solicitudes.at[idx, 'id_plaza_asignada'] = id_plaza_asignada
        plazas_asignadas.add(id_plaza_asignada)

#Mensaje de plazas asignadas
plazas_asignadas = disponibilidad['asignada'].sum()
plazas_no_asignadas = disponibilidad.shape[0] - plazas_asignadas

#Preparecion de las id_plaza para subir a tabla scoring
df_id_plaza = solicitudes[['id_solicitud', 'id_plaza_asignada']]
df_id_plaza = df_id_plaza.rename(columns={'id_plaza_asignada': 'id_plaza'})


print(f'Plazas asignadas: {plazas_asignadas}\n')
print(f'Plazas no asignadas: {plazas_no_asignadas}\n')



# In[166]:


# Subir asignacion de plaza a tabla score
# Intentar establecer la conexión
try:
    with psycopg2.connect(connection_string) as connection:
        # Obtener el cursor
        cursor = connection.cursor()

        # Actualizar la columna 'id_plaza'
        for _, row in df_id_plaza.iterrows():
            update_query = sql.SQL("UPDATE scoring SET id_plaza = {} WHERE id_solicitud = {}").format(
                sql.Placeholder(), sql.Placeholder()
            )
            cursor.execute(update_query, (row['id_plaza'], row['id_solicitud']))

        # Confirmar la transacción
        connection.commit()

        print("Datos de 'id_plaza' actualizados correctamente en la tabla scoring.\n")

except psycopg2.Error as e:
    print(f"Error al actualizar datos en la columna 'id_plaza' de la tabla scoring: {e}")


# ### >>>ACTUALIZAR scoring.py

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




