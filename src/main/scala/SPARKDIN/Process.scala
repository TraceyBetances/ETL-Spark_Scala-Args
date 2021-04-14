package SPARKDIN

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import SPARKDIN.Funciones._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter



object Process extends Structure {


  override def dataflow(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("SDG-Prueba")
      .enableHiveSupport
      .getOrCreate()


    val readDF = spark.read.format(tipoArchivoLectura(args)).load(pathEntrada(args))

    val dfOK = readDF
      .where(campoNoVacio(args) + " <> ''")
      .where(campoNotNull(args) + " IS NOT NULL")
      .withColumn(timeStampCol(args), from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfOK")
    dfOK.show(20, truncate = false)

    val dfKOVacio = readDF
      .where(campoNoVacio(args) + " = ''")
      .withColumn(nuevaColumnaError(args), lit("Error01:Campo vacio"))
      .withColumn(timeStampCol(args), from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfKOVacio")
    dfKOVacio.show(20, truncate = false)

    val dfKONull = readDF
      .where(campoNotNull(args) + " IS NULL")
      .withColumn(nuevaColumnaError(args), lit("Error02:Campo nulo"))
      .withColumn(timeStampCol(args), from_utc_timestamp(current_timestamp().cast(StringType), "Europe/Madrid").cast(StringType))

    println("dfKONull")
    dfKONull.show(20, truncate = false)

    val dfErrores = dfKOVacio.union(dfKONull)
    println("dfErrores")
    dfErrores.show(20, truncate = false)

    dfOK.repartition(1).write.format(tipoArchivoSalida(args)).mode(tipoGuardado(args)).save(pathSalidaOK(args))
    dfErrores.repartition(1).write.format(tipoArchivoSalida(args)).mode(tipoGuardado(args)).save(pathSalidaKO(args))

  }

  def main(args: Array[String]): Unit = {

    dataflow(args)

  }

}
