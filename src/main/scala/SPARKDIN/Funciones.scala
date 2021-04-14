package SPARKDIN

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object Funciones {

    def tipoArchivoLectura(args: Array[String]): String = args(0)

    def pathEntrada(args: Array[String]): String = args(1)

    def campoNoVacio(args: Array[String]): String = args(2)

    def campoNotNull(args: Array[String]): String = args(3)

    def timeStampCol(args: Array[String]): String = args(4)

    def tipoArchivoSalida(args: Array[String]): String = args(5)

    def tipoGuardado(args: Array[String]): String = args(6)

    def pathSalidaOK(args: Array[String]): String = args(7)

    def pathSalidaKO(args: Array[String]): String = args(8)

    def nuevaColumnaError(args: Array[String]): String = args(9)

  }
