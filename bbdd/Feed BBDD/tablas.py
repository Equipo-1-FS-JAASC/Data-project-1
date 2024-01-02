import pandas as pd
import numpy as np
from datetime import datetime
#Se usa r delante del string para evitar que interprete las barras como comandos de salida de código
#IMPORTANTE: Para que funcione, cambia las rutas en rojo donde estarán los archivos. Todo parte de df cuyo csv descargado de Mockaroo tiene que llamarse USUARIOS_RAW.csv
df=pd.read_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\USUARIOS_RAW.csv')

def preparacion_tabla_usuario():
    #CREAR CAMPO EDAD
    df['Fecha de Nacimiento'] = pd.to_datetime(df['Fecha de Nacimiento'], format='%d/%m/%Y')
    df['Edad'] = df['Fecha de Nacimiento'].apply(lambda x: datetime.now().year - x.year - ((datetime.now().month, datetime.now().day) < (x.month, x.day)))
    #NORMALIZAR VALOR SOLICIANTE PARA QUE NO SEA 50/50
    total_filas = len(df)
    objetivo_trues = int(0.65 * total_filas)
    trues_actuales = df['Usuario_solicitante'].sum()
    trues_a_agregar = objetivo_trues - trues_actuales
    indices_falses = df[df['Usuario_solicitante'] == False].index
    if trues_a_agregar > 0:
        indices_a_cambiar = np.random.choice(indices_falses, trues_a_agregar, replace=False)
        df.loc[indices_a_cambiar, 'Usuario_solicitante'] = True
    #DEPURAR ID_SOLICITUD (Para que todos los que tengan False estén asociados a un ID Solicitud existente)
    df['ID Solicitud'] = df['ID Solicitud'].astype(str)
    solicitantes = df[df['Usuario_solicitante'] == True]['ID Solicitud'].unique()
    acompanantes = df[df['Usuario_solicitante'] == False].copy()
    for i, acompanante in acompanantes.iterrows():
        for id_solicitante in solicitantes:
            if df[df['ID Solicitud'] == id_solicitante].shape[0] < 2:
                df.at[i, 'ID Solicitud'] = id_solicitante
                break
    if df[df['Usuario_solicitante'] == False].shape[0] > df[df['Usuario_solicitante'] == True].shape[0]:
        ids_unicos_acompanantes = df[df['Usuario_solicitante'] == False]['ID Solicitud'].unique()
        for id_acompanante in ids_unicos_acompanantes:
            if df[df['ID Solicitud'] == id_acompanante].shape[0] < 2:
                df.loc[(df['ID Solicitud'] == id_acompanante) & (df['Usuario_solicitante'] == False), 'Usuario_solicitante'] = True
            break
    #GUARDAR ARCHIVO
    df.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_USUARIOS.csv', index=False)

def preparacion_tabla_renta():
    df_ingresos = pd.DataFrame()
    df_ingresos ['DNI'] = df['DNI']
    df_ingresos['Ingresos'] = np.round(np.random.uniform(484.61, 3400, size=len(df)), 2)
    df_ingresos.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_RENTA.csv', index=False)

def preparacion_tabla_discapacidad():
    df_discapacidad = pd.DataFrame()
    df_discapacidad ['DNI'] = df['DNI']
    grados = [0, 1, 2, 3, 4]
    probabilidades = [0.4, 0.4, 0.15, 0.025, 0.025]
    df_discapacidad['Grado_dis'] = np.random.choice(grados, size=len(df), p=probabilidades)
    df_discapacidad.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_DISCAPACIDAD.csv', index=False)

def preparacion_tabla_patrimonio():
    df_patrimonio = pd.DataFrame()
    df_patrimonio ['DNI'] = df['DNI']
    rangos = ['0-20000','20001-60000', '60001-150000','150001-250000','250000-375000', '375001-499999', '500000-3000000']
    probabilidades = [0.1, 0.15, 0.35, 0.33, 0.04, 0.02 ,0.01]
    def generar_valor(rango):
        if rango == '0-20000':
            return np.random.randint(0, 20000)
        elif rango == '20001-60000':
            return np.random.randint(20001,60000)
        elif rango == '60001-150000':
            return np.random.randint(60001,150000)
        elif rango == '150001-250000':
            return np.random.randint(150001,250000)
        elif rango == '250000-375000':
            return np.random.randint(250000,375000)
        elif rango == '375001-499999':
            return np.random.randint(375001,499999)
        elif rango == '500000-3000000':
            return np.random.randint(500000,3000000)
    df_patrimonio['Valoracion_Patrimonio'] = [generar_valor(np.random.choice(rangos, p=probabilidades)) for _ in range(len(df))]
    df_patrimonio.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_PATRIMONIO.csv', index=False)

def preparacion_tabla_familia_num():
    df_familia_num = pd.DataFrame()
    df_familia_num ['DNI'] = df['DNI']
    valores = [0, 1, 2]
    probabilidades = [0.75, 0.15, 0.10]
    df_familia_num['Grado_dis'] = np.random.choice(valores, size=len(df), p=probabilidades)
    df_familia_num.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_FAMILIA_NUMEROSA.csv', index=False)

def preparacion_historial_usuarios():
    df_familia_num = pd.DataFrame()
    df_familia_num ['DNI'] = df['DNI']
    valores = [0, 1, 2]
    probabilidades = [0.75, 0.15, 0.10]
    df_familia_num['Grado_dis'] = np.random.choice(valores, size=len(df), p=probabilidades)
    df_familia_num.to_csv(r'G:\Mi unidad\EDEM\DATA PROJECT 1\FICHEROS FEED\TABLA_FAMILIA_NUMEROSA.csv', index=False)

preparacion_tabla_usuario()
preparacion_tabla_renta()
preparacion_tabla_discapacidad()
preparacion_tabla_patrimonio()
preparacion_tabla_familia_num()

