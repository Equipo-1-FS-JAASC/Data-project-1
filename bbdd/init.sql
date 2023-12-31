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
DROP TABLE IF EXISTS autorechazos;



-- Crear tabla solicitudes
CREATE TABLE solicitudes (
    id_solicitud INT PRIMARY KEY,
    anyo_solicitud INT,
    usuarios_sol VARCHAR(255),
    renta_sol INT,
    patrimonio_sol INT,
    primera_opcion VARCHAR(255),
    fecha_1op DATE,
    segunda_opcion VARCHAR(255),
    fecha_2op DATE,
    tercera_opcion VARCHAR(255),
    fecha_3op DATE
);


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
    scoring_ind_participacion_previa FLOAT,
    FOREIGN KEY (dni) REFERENCES usuarios(dni)
);

-- Crear tabla renta
CREATE TABLE renta (
    dni VARCHAR(9) PRIMARY KEY,
    ingresos FLOAT,
    FOREIGN KEY (dni) REFERENCES usuarios(dni)
);

-- Crear tabla discapacidad
CREATE TABLE discapacidad (
    dni VARCHAR(9) PRIMARY KEY,
    grado_dis INT,
    FOREIGN KEY (dni) REFERENCES usuarios(dni)
);

-- Crear tabla familia_numerosa
CREATE TABLE familia_numerosa (
    dni VARCHAR(9) PRIMARY KEY,
    tipo_fam_num INT,
    FOREIGN KEY (dni) REFERENCES usuarios(dni)
);

-- Crear tabla patrimonio
CREATE TABLE patrimonio (
    dni VARCHAR(9) PRIMARY KEY,
    valoracion_patrimonio FLOAT,
    coche BOOLEAN,
    arrendador BOOLEAN,
    FOREIGN KEY (dni) REFERENCES usuarios(dni)
);


-- Crear tabla hoteles
CREATE TABLE hoteles (
    id_hotel VARCHAR(9) PRIMARY KEY,
    nombre VARCHAR(255),
    localizacion VARCHAR(255)
);

-- Crear tabla disponibilidad
CREATE TABLE disponibilidad (
    id_plaza VARCHAR(9),
    id_hotel VARCHAR(255),
    fecha_disponibilidad_hab DATE ,
    num_hab_disp INT,
    PRIMARY KEY (id_plaza),
    FOREIGN KEY (id_hotel) REFERENCES hoteles(id_hotel)
);


-- Crear tabla scoring
CREATE TABLE scoring (
    id_solicitud INT PRIMARY KEY,
    id_plaza VARCHAR(9), 
    index_ INT,
    score FLOAT,
    edad_score FLOAT,
    discapacidad_score FLOAT,
    coche_score FLOAT,
    arrendador_score FLOAT,
    oficio_especial_score FLOAT,
    familia_score FLOAT,
    renta_score FLOAT,
    part_previa_score FLOAT,
    patrimonio_score FLOAT,
    FOREIGN KEY (id_solicitud) REFERENCES solicitudes(id_solicitud),
    FOREIGN KEY (id_plaza) REFERENCES disponibilidad(id_plaza)

);


CREATE TABLE autorechazos (
    dni VARCHAR(9) PRIMARY KEY,
    nombre VARCHAR(255),
    apellido VARCHAR(255),
    edad INT,
    fecha_de_nacimiento DATE,
    id_solicitud INT
);

