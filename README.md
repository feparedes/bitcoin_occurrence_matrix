# Descripción

Este es un trabajo fin de máster perteneciente al máster de ingeniería informática a distancia de la Universidad de Sevilla. Tratamos de desarrollar una ETL que se encargue de tratar los datos de un dataset de la blockchain de bitcoin para poder obtener finalmente la denominada matriz de ocurrencias de bitcoin. Esta matriz contiene la información de como se comporta la blockchain en base a las direcciones de entrada y salida de cada transacción teniendo en cuenta que los grafos son de orden *k*, donde *k* es un parámetro que se puede modificar.

# Ejemplo de uso

Para llevar a cabo la réplica de este proyecto son necesarios los siguientes pasos.
1. Realizar un clonado de este proyecto a local con `git clone`

2. Situarse en el directorio del proyecto y comprobar que están los archivos de dockerfile y docker compose.

3. Tener un fichero (privado) `.env` que se encuentre en el mismo directorio que los archivos de docker donde vengan las siguientes variables

`PROFILE_TARGET=prod`

`POSTGRES_HOST=********`

`DBT_PROFILES_DIR=/project/dbt`

`POSTGRES_PORT=********`

`POSTGRES_USER=********`

`POSTGRES_PASSWORD=********`

`POSTGRES_DBNAME=********`

`POSTGRES_SCHEMA=********`

donde las variables con asteriscos deben ser rellenas con una base de datos. En nuestro caso se ha empleado una base de dato de OVH. No se ha podido hacer en local por la magnitud de los modelos que se han generado en DBT.

4. En la carpeta `raw_data/` es necesario descomprimir el archivo con la información de la blockchain de bitcoin.

5. Levantar el servicio de `airflow_init` con docker mediante el comando `docker compose up airflow_init`.

6. Una vez termine ejecutamos el comando `docker compose up`

7. Esto último levantará los servicios necesarios que desplegará airflow para poder trabajar con él. Para ello nos iremos a una ventana y escribiremos `localhost:8080`. Esto nos llevará a la interfaz web de airflow. Alli podremos ir al dag que se llama `occurence_matrix`.

8. Una vez en este dag ejecutamos la pipeline y directamente podemos ver como comienza nuestra ETL.

9. Una vez haya terminado podemos irnos al log de la tarea `display_occurrence_matrix` y ver el resultado final.