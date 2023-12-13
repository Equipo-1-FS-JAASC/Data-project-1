# Objeto del Repositiorio
Este es el repositorio relativo al primer Data Project del Máster de Data Analytics de la promoción 2023-24. El grupo está compuesto por Stanislav, Adriana, Cristian, Alberto y Juan.

# Índice
- [Objeto del Repositiorio](#objeto-del-repositiorio)
- [Índice](#índice)
- [Objetivo del Data Project](#objetivo-del-data-project)
- [Flujograma del proceso](#flujograma-del-proceso)
- [Adaptación del proceso - Nuestra visión de la justicia](#adaptación-del-proceso---nuestra-visión-de-la-justicia)
- [Adaptación del proceso - Adaptaciones propuestas](#adaptación-del-proceso---adaptaciones-propuestas)
- [Estructura BBDD - Modelo Relacional](#estructura-bbdd---modelo-relacional)
- [Estructura BBDD - Listado y descripción de campos](#estructura-bbdd---listado-y-descripción-de-campos)

# Objetivo del Data Project
Hacer una revisión del proceso actual de asignación de plazas del Imserso y ofrecer una versión que sea más justa en base a nuestro propio criterio.

# Flujograma del proceso
El primer paso es evaluar el modelo actual, para ver si se adapta a nuestra consideración de justicia. Para ello, el equipo ha realizado un exhaustivo análisis del proceso actual, para identificar los posibles puntos de mejora.

El detalle del proceso se encuentra en el siguiente pdf adjunto: [Imserso Process 1.2.pdf](https://github.com/Equipo-1-FS-JAASC/Data-project-1/files/13512478/Imserso.Process.1.2.pdf)

# Adaptación del proceso - Nuestra visión de la justicia
Nuestra visión entiende la realidad de cada individuo como una realidad con múltiples variables. En tanto y en cuanto más variables adaptemos al modelo, mejor podrá estimar la situación real de cada individuo y, por tanto, más justo se podrá ser en cuanto a la asignación de plazas.

| VARIABLE INCLUIDAS EN EL SCORING | OBJETIVO |
|----------------------------------|----------|
| Patrimonio de los usuarios | La riqueza neta media actual de los españoles según el INE es de 269.000€, y la mediana 122.000€. Con el objetivo de ofrecer el servicio a aquellos con menor acceso a este tipo de ofertas, se han excluido a algunos usuarios con alto patrimonio. No obstante, para este tipo de usuarios se les ha habilitado un marcador para ofrecerles un producto alternativo. |
| Coche | Hemos considerado una variable interesante el hecho de que el usuario solicitante disponga de un coche a su nombre. El objetivo es poder favorecer a aquellos usuarios que no dispongan de coche para viajar, ya que en comparación, se podrían encontrar en una posición significativamente más limitada para realizar viajes de forma regular. |
| Alquiler vivienda | De la misma forma, nuestro modelo también tiene en cuenta si el usuario solicitante disfruta de una renta como arrendatario de una vivienda diferente a la habitual. En tal caso, deberá de tener menos puntuación que aquellos que no dispongan de este tipo de renta. |
| Tipo de trabajo | Consideramos que aquellas personas que hayan desempeñado un trabajo especial para la sociedad, deben de tener una ligera bonificación. |
| Grado de discapacidad | Se han mejorado las ponderaciones por discapacidad 1 y 2. |

# Adaptación del proceso - Adaptaciones propuestas
![Imagen_adaptaciones_propuestas](https://github.com/Equipo-1-FS-JAASC/Data-project-1/assets/145840791/f38dc2d6-81ed-407d-b8e8-00e199bc67fe)

# Estructura BBDD - Modelo Relacional
En base a las nuevas adaptaciones del proceso, hemos diseñado una estructura de BBDD para que se ajuste a este nuevo proceso.

El detalle del proceso se encuentra en el siguiente pdf adjunto: [Imserso Process 1.2.pdf](https://github.com/Equipo-1-FS-JAASC/Data-project-1/files/13512478/Imserso.Process.1.2.pdf)

# Estructura BBDD - Listado y descripción de campos
El detalle de cada tabla y campo se encuentra en el siguiente documento : [tabla campos  & origen datos.xlsx](https://github.com/Equipo-1-FS-JAASC/Data-project-1/files/13660962/tabla.campos.origen.datos.xlsx)
