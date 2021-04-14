En este proceso se contruye la aplicación con un jar.
El main llama al método DataFlow al que le pasamos el parámetro 'args' donde estan los argumentos de entrada del SparkSubmit.
Creamos un DataFrame con la lectura del input.
Creamos un DataFrame OK que contiene los registros completos. En este DataFrame creamos una condición 'where' ara quedarnos con los NO vacios y los NO nulos y 
le añadimos la columna timestamp.
Creamos el DataFrame KOVacio que contiene los registros vacios y le añadimos la columna de timestamp, además de tipo de error ("Error01:Campo vacio").
Creamos el DataFrame KONulos que contiene los registros nulos y le añadimos la columna de timestamp, además de tipo de error ("Error02:Campo nulo").
Estos dos ultimos DataFrames con los registros que no han pasado las validaciones se unen en un unico DataFrame.
Por ultimo, se graban los DataFrame OK y KO en las rutas especificadas.

El SparkSubmit debe contener todos los argumentos de entrada.

spark-submit --master local[*] --class SPARKDIN.Process target/ETL-ARGS-PRUEBA-1.0-SNAPSHOT-shaded.jar tipoArchivoLectura pathEntrada campoNoVacio campoNotNull
timeStampCol tipoArchivoSalida tipoGuardado pathSalidaOK pathSalidaKO arrnuevaColumnaError
