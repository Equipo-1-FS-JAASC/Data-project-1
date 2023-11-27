# BBDD Postgres

## Procedimiento de Configuración de la Base de Datos: Detalles sobre la Implementación"


En el archivo de configuración Docker Compose, podemos observar la secuencia de pasos que seguimos durante el desarrollo del proyecto. Primero creamos la base de datos, insertamos los datos de en las tablas. Luego ejecutamos nuestro algoritmo diseñado para realizar el sorteo, aplicando la lógica necesaria para determinar quiénes serán los beneficiarios del programa IMSERSO. Por último, culminamos el proceso obteniendo la tabla que identifica a los usuarios elegibles para disfrutar de los beneficios del IMSERSO.

Primero, llevamos a cabo la creación de una base de datos utilizando **PostgreSQL**. Este proceso inicial fue fundamental para establecer el entorno de almacenamiento de datos necesario para nuestro proyecto, en nombre de la bbdd es dbImserso. 




