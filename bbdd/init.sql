-- Eliminar tablas si existen
DROP TABLE IF EXISTS usuarios;
DROP TABLE IF EXISTS historial_usuarios;
DROP TABLE IF EXISTS renta;
DROP TABLE IF EXISTS scoring;
DROP TABLE IF EXISTS tipo_resolucion;
DROP TABLE IF EXISTS disponibilidad;
DROP TABLE IF EXISTS discapacidad;
DROP TABLE IF EXISTS familia_numerosa;
DROP TABLE IF EXISTS patrimonio;
DROP TABLE IF EXISTS hoteles;
DROP TABLE IF EXISTS destinos;
DROP TABLE IF EXISTS resolucion_solicitudes;
DROP TABLE IF EXISTS solicitudes;



-- Crear tabla usuarios
CREATE TABLE usuarios (
    dni VARCHAR(9) PRIMARY KEY,
    nombre VARCHAR(255),
    apellido VARCHAR(255),
    edad INT,
    fecha_de_nacimiento DATE,
    id_solicitud INT,
    usuario_solicitante BOOLEAN,
    oficio_especial BOOLEAN
);

-- Crear tabla historial_usuarios
CREATE TABLE historial_usuarios (
    dni VARCHAR(9) PRIMARY KEY,
    cross_selling BOOLEAN,
    resultado_solicitud_t_1 INT,
    viajes_t_1 INT,
    resultado_solicitud_t_2 INT,
    viajes_t_2 INT,
    suma_viajes_t_1_y_t_2 INT,
    scoring_ind_participacion_previa FLOAT
);

-- Crear tabla renta
CREATE TABLE renta (
    dni VARCHAR(9) PRIMARY KEY,
    ingresos FLOAT
);


-- Crear tabla scoring
CREATE TABLE scoring (
    id_solicitud VARCHAR(9) PRIMARY KEY,
    total FLOAT,
    edad FLOAT,
    discapacidad FLOAT,
    ingresos FLOAT,
    agregado_part_previa FLOAT,
    fam_num FLOAT
);

-- Crear tabla tipo_resolucion
CREATE TABLE tipo_resolucion (
    id_resolucion INT PRIMARY KEY,
    tipo_resolucion VARCHAR(255)
);

-- Crear tabla disponibilidad
CREATE TABLE disponibilidad (
    id_hotel VARCHAR(255)  PRIMARY KEY,
    fecha_disponibilidad_hab DATE,
    num_hab_disp INT
);

-- Crear tabla discapacidad
CREATE TABLE discapacidad (
    dni VARCHAR(9) PRIMARY KEY,
    grado_dis INT
);

-- Crear tabla familia_numerosa
CREATE TABLE familia_numerosa (
    dni VARCHAR(9) PRIMARY KEY,
    tipo_fam_num INT
);

-- Crear tabla patrimonio
CREATE TABLE patrimonio (
    dni VARCHAR(9) PRIMARY KEY,
    valoracion_patrimonio FLOAT,
    coche BOOLEAN,
    arrendador BOOLEAN
);

-- Crear tabla hoteles
CREATE TABLE hoteles (
    id_hotel INT PRIMARY KEY,
    nombre VARCHAR(255),
    ciudad VARCHAR(255)
);

CREATE TABLE destinos (
    id_destinos INT PRIMARY KEY,
    tipo_destino VARCHAR(255),
    plazas_destino INT
    );

CREATE TABLE resolucion_solicitudes (
    id_solicitud INT PRIMARY KEY,
    anyo_solicitud INT,
    usuarios_sol VARCHAR(255),
    renta_sol INT,
    primera_opcion VARCHAR(255),
    segunda_opcion VARCHAR(255),
    tercera_opcion VARCHAR(255)
);

-- Crear tabla solicitudes
CREATE TABLE solicitudes (
    id_solicitud INT PRIMARY KEY,
    anyo_solicitud INT,
    usuarios_sol VARCHAR(255),
    renta_sol INT,
    primera_opcion VARCHAR(255),
    fecha_1op DATE,
    segunda_opcion VARCHAR(255),
    fecha_2op DATE,
    tercera_opcion VARCHAR(255),
    fecha_3op DATE
);