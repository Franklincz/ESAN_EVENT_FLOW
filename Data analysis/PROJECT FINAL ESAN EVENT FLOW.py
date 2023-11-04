# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ##Leer tabla principal

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`reporte_event_flow`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREAMOS EL WIDGET

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE WIDGET COMBOBOX FECHA_SELEC DEFAULT "2023-10-03" CHOICES SELECT DISTINCT FECHA FROM `hive_metastore`.`default`.`reporte_event_flow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## EJECUTAMOS NUESTRO PRIMER GRAFICO

# COMMAND ----------

#### CREAMOS LA VARIABLE PARA ALMACENAR LA FECHA SELECCIONADA

# COMMAND ----------

fecha_selec=getArgument("FECHA_SELEC")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Sexo, COUNT(*) AS cantidad
# MAGIC FROM `hive_metastore`.`default`.`reporte_event_flow`
# MAGIC WHERE Fecha =getArgument("FECHA_SELEC")
# MAGIC GROUP BY Sexo

# COMMAND ----------

# MAGIC %md
# MAGIC # PARA DATA DE  FECHA TAL 

# COMMAND ----------

# MAGIC %md
# MAGIC # Importa las bibliotecas necesarias
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Crea una sesión de Spark
# MAGIC spark = SparkSession.builder.appName("EjemploSpark").getOrCreate()
# MAGIC
# MAGIC # Ejecuta una consulta SQL en la tabla de Databricks y obtén los resultados
# MAGIC consulta_sql = "SELECT * FROM `hive_metastore`.`default`.`reporte_event_flow` WHERE FECHA = '2023-10-03'"
# MAGIC resultado_spark = spark.sql(consulta_sql)
# MAGIC
# MAGIC # Convierte el resultado en un DataFrame de Pandas
# MAGIC df_pandas_2023_10_03 = resultado_spark.toPandas()
# MAGIC
# MAGIC # Ahora, 'df_pandas' es un DataFrame de Pandas que contiene los datos de la tabla de Databricks
# MAGIC df_pandas_2023_10_03['Fecha'] = pd.to_datetime(df_pandas_2023_10_03['Fecha'])
# MAGIC df_pandas_2023_10_03['Hora'] = pd.to_timedelta(df_pandas_2023_10_03['Hora'])  # Convierte la columna de hora a formato timedelta
# MAGIC
# MAGIC # Combina las columnas de fecha y hora en una sola columna de fecha y hora
# MAGIC df_pandas_2023_10_03['Fecha_Hora'] = df_pandas_2023_10_03['Fecha'] + df_pandas_2023_10_03['Hora']
# MAGIC df_pandas_2023_10_03['Fecha_Hora'].head()
# MAGIC
# MAGIC # Define la fecha máxima y mínima en el DataFrame
# MAGIC # Define la fecha máxima y mínima en el DataFrame
# MAGIC fecha_maxima = df_pandas_2023_10_03['Fecha_Hora'].max()
# MAGIC fecha_minima = df_pandas_2023_10_03['Fecha_Hora'].min()
# MAGIC df_pandas_2023_10_03['Fecha_Hora'] = pd.to_datetime(df_pandas_2023_10_03['Fecha_Hora'])
# MAGIC # Establece la columna 'Fecha_Hora' como índice
# MAGIC df_pandas_2023_10_03.set_index('Fecha_Hora', inplace=True)
# MAGIC df_pandas_2023_10_03 = df_pandas_2023_10_03[['Sexo']]
# MAGIC # Utiliza la función groupby para contar la cantidad de mujeres y varones en intervalos de 5 minutos
# MAGIC conteo_por_mujeres = df_pandas_2023_10_03[df_pandas_2023_10_03['Sexo'] == 'Femenino'].groupby(pd.Grouper(freq='5T')).size()
# MAGIC conteo_por_varones = df_pandas_2023_10_03[df_pandas_2023_10_03['Sexo'] == 'Masculino'].groupby(pd.Grouper(freq='5T')).size()
# MAGIC # Utiliza la función resample para contar la cantidad de veces que aparece 'Sexo' en intervalos de 5 minutos
# MAGIC conteo_por_intervalo = df_pandas_2023_10_03['Sexo'].resample('5T').count()
# MAGIC # Asigna el resultado de resample de nuevo al DataFrame original
# MAGIC df_pandas_2023_10_03 = pd.DataFrame({'Conteo_Sexo': conteo_por_intervalo})
# MAGIC # Agrega las columnas de conteo de mujeres y varones al DataFrame original
# MAGIC df_pandas_2023_10_03['MujeresCantidad'] = conteo_por_mujeres
# MAGIC df_pandas_2023_10_03['VaronesCantidad'] = conteo_por_varones
# MAGIC df_pandas_2023_10_03 = df_pandas_2023_10_03.reset_index()
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

def procesar_datos_por_fecha(fecha_seleccionada):
    # Crea una sesión de Spark
    spark = SparkSession.builder.appName("EjemploSpark").getOrCreate()

    # Ejecuta una consulta SQL en la tabla de Databricks y obtén los resultados
    consulta_sql = f"SELECT * FROM `hive_metastore`.`default`.`reporte_event_flow` WHERE FECHA = '{fecha_seleccionada}'"
    resultado_spark = spark.sql(consulta_sql)

    # Convierte el resultado en un DataFrame de Pandas
    df_pandas = resultado_spark.toPandas()

    # Convierte las columnas 'Fecha' y 'Hora' a tipos datetime y timedelta
    df_pandas['Fecha'] = pd.to_datetime(df_pandas['Fecha'])
    df_pandas['Hora'] = pd.to_timedelta(df_pandas['Hora'])

    # Combina las columnas de fecha y hora en una sola columna de fecha y hora
    df_pandas['Fecha_Hora'] = df_pandas['Fecha'] + df_pandas['Hora']

    # Define la fecha máxima y mínima en el DataFrame
    fecha_maxima = df_pandas['Fecha_Hora'].max()
    fecha_minima = df_pandas['Fecha_Hora'].min()

    # Establece la columna 'Fecha_Hora' como índice
    df_pandas.set_index('Fecha_Hora', inplace=True)
    df_pandas = df_pandas[['Sexo']]

    # Utiliza la función groupby para contar la cantidad de mujeres y varones en intervalos de 5 minutos
    conteo_por_mujeres = df_pandas[df_pandas['Sexo'] == 'Femenino'].groupby(pd.Grouper(freq='5T')).size()
    conteo_por_varones = df_pandas[df_pandas['Sexo'] == 'Masculino'].groupby(pd.Grouper(freq='5T')).size()

    # Utiliza la función resample para contar la cantidad de veces que aparece 'Sexo' en intervalos de 5 minutos
    conteo_por_intervalo = df_pandas['Sexo'].resample('5T').count()

    # Asigna el resultado de resample de nuevo al DataFrame original
    df_pandas = pd.DataFrame({'Conteo_Sexo': conteo_por_intervalo})

    # Agrega las columnas de conteo de mujeres y varones al DataFrame original
    df_pandas['MujeresCantidad'] = conteo_por_mujeres
    df_pandas['VaronesCantidad'] = conteo_por_varones
    df_pandas = df_pandas.reset_index()
    # Convierte el DataFrame de Pandas en un DataFrame Spark
    df_spark = spark.createDataFrame(df_pandas)

    # Escribe el DataFrame Spark en una tabla en Databricks
    tabla_destino = f"REGISTROS{fecha_seleccionada.replace('-', '')}"
    df_spark.write.mode("overwrite").saveAsTable(tabla_destino)


    return df_pandas




# COMMAND ----------

procesar_datos_por_fecha(fecha_selec)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HACEMOS CONSULTAS A LAS TABLAS

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## CONSULTA PARA MOSTRAR LOS GRAFICOS

# COMMAND ----------

# Construye el nombre de la tabla REGISTROS basado en la fecha seleccionada
nombre_tabla = 'REGISTROS' + fecha_selec.replace('-', '')

# COMMAND ----------


# Construye la consulta SQL como una cadena
consulta_sql = f'SELECT * FROM `hive_metastore`.`default`.`{nombre_tabla}` WHERE DATE_FORMAT(Fecha_Hora, "yyyy-MM-dd") = "{fecha_selec}"'
#(Fecha_Hora,'yyyy-MM-dd')
# Ejecuta la consulta SQL generada
spark.sql(consulta_sql)

# Crea una vista temporal a partir del DataFrame
spark.sql(consulta_sql).createOrReplaceTempView("resultados_temp")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ejecuta una consulta SQL para seleccionar y mostrar los datos
# MAGIC SELECT * FROM resultados_temp  ORDER BY FECHA_HORA DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC IF getArgument("FECHA_SELEC") = '2023-10-03' THEN
# MAGIC   SELECT * FROM REGISTROS20231003
# MAGIC ELSEIF getArgument("FECHA_SELEC") = '2023-09-28' THEN
# MAGIC   SELECT * FROM REGISTROS20230928
# MAGIC --ELSEIF getArgument("FECHA_SELEC") = '2023-09-29' THEN
# MAGIC  -- SELECT * FROM REGISTROS20230929
# MAGIC --ELSEIF getArgument("FECHA_SELEC") = '2023-09-11' THEN
# MAGIC  --SELECT * FROM REGISTROS20230911
# MAGIC -- Agrega más casos aquí según sea necesario para otras fechas
# MAGIC ELSE
# MAGIC   SELECT NULL AS Col1, NULL AS Col2 -- Otra acción en caso de que la fecha no coincida con ningún caso
# MAGIC END;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -- Define la fecha seleccionada
# MAGIC SET FECHA_SELEC = getArgument("FECHA_SELEC");
# MAGIC
# MAGIC -- Consulta SQL dinámica para seleccionar la tabla en función de la fecha
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TablaSeleccionada AS
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN '${FECHA_SELEC}' = '2023-10-03' THEN 'REGISTROS20231003'
# MAGIC     WHEN '${FECHA_SELEC}' = '2023-09-28' THEN 'REGISTROS20230928'
# MAGIC     WHEN '${FECHA_SELEC}' = '2023-09-29' THEN 'REGISTROS20230929'
# MAGIC     WHEN '${FECHA_SELEC}' = '2023-09-11' THEN 'REGISTROS20230911'
# MAGIC     -- Agrega más casos aquí según sea necesario para otras fechas
# MAGIC     ELSE NULL
# MAGIC   END AS tabla;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -- Ejecuta una consulta en la tabla seleccionada
# MAGIC SELECT * FROM TablaSeleccionada;

# COMMAND ----------

# MAGIC %md
# MAGIC #GRAFICOS DE DASHBOARD 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sum(CONTEO_SEXO) AS `CANTIDAD TOTAL`
# MAGIC FROM resultados_temp
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(VaronesCantidad)) AS Media_VaronesCantidad,
# MAGIC   round(MODE(VaronesCantidad)) AS Moda_VaronesCantidad
# MAGIC FROM resultados_temp
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## PORCENTAJE DE VARONES Y MUJEREDS

# COMMAND ----------

# MAGIC %md
# MAGIC /*
# MAGIC SELECT
# MAGIC   CAST((SUM(MujeresCantidad) * 100.0) / (SUM(MujeresCantidad) + SUM(VaronesCantidad)) AS VARCHAR) || '%' AS Porcentaje_Mujeres,
# MAGIC   SUM(MujeresCantidad)
# MAGIC FROM resultados_temp;
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SUM(VaronesCantidad) * 100.0) / (SUM(MujeresCantidad) + SUM(VaronesCantidad)) AS Porcentaje_Varones
# MAGIC   FROM resultados_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('HOUR', FECHA_HORA) AS HORA,
# MAGIC   ROUND(AVG(VaronesCantidad),0) AS Promedio_Varones,
# MAGIC   ROUND(AVG(MujeresCantidad),0)  AS Promedio_Mujeres
# MAGIC FROM resultados_temp
# MAGIC GROUP BY HORA
# MAGIC ORDER BY HORA;
# MAGIC
# MAGIC
