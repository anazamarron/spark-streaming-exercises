/*
 * Copyright (c) 2015 Daniel Higuero.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.spark.examples.streaming

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Code here the solution for the proposed exercises.
 */
object ejercicio4 {

  /**
   * Field separator.
   */
  val Separator = ";";

  /**
   * Threshold that determines when a number of failed auth entries is considered an attack.
   */
  val ThresholdAuth = 1;

  /**
   * Threshold that determines when a number of failed web access entries is considered an attack.
   */
  val ThresholdWeb = 1;

  def main(args: Array[String]): Unit = {
    //Suppress Spark output
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Define the Spark configuration. In this case we are using the local mode
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ReadingLogs_exercise2")
    //Define a SparkStreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(10))



    //Este no lo uso. Lo cargo en caché para que no me de un error de java que estropea el resultado
    val events = ssc.socketTextStream("localhost", 10002, StorageLevel.MEMORY_AND_DISK_SER)
    //Filter out empty lines and print the count
    val numberEventsRDD = events.map(x => {
      val arr = x.split(';')
      new WebEvent(arr(0), arr(1), arr(2), arr(3), arr(4))
    }).foreachRDD(x=>x.cache())

    // Voy a usar el de las autorizaciones. hago un print para que me de los tipos de lineas

    // Given the previous stream obtained in Exercise 3, filter those hosts that are already blacklisted in the system.
    // In order to do that, generate an RDD with the contents of a file that contains:


    val autorizacion = ssc.socketTextStream("localhost", 10001, StorageLevel.MEMORY_AND_DISK_SER)
    val numberEvents_authRDD = autorizacion.map(x => {
      val arr = x.split(';')
      new AuthEvent(arr(0), arr(1), arr(2), arr(3))
    })


    //Como del ejercicio anterior me salieron los hosts 3 5 6 y 9, los voy a meter en un RDD


    val lista = ssc.sparkContext.parallelize(List("host3","host5","host6","host9"))
    val atacantes = lista.map(x => (x,0))

    //Obtengo la fecha del sistema para imprimirla

    val today = Calendar.getInstance().getTime()

    //primero filtro los que son ataque y los sumo

    val comunRDD =numberEvents_authRDD.filter(x => x.Message.contains("failed"))
      .map(x => (x.Source, 1))
      .reduceByKeyAndWindow((acum,nuevo)=>acum+nuevo,Seconds(10))
      .transform(rdd => rdd.join(atacantes).filter(x => x._2._2 == 0).map(x => (x._1,x._2._1)))
      .foreachRDD(rdd => {
      println()
      println(today)

      println("Numero total de ataques por los servidores de la lista: " + rdd.reduce((x, y) => ("Total", x._2 + y._2))._2)
      println()
      println("Numero de servidores que atacan: " + rdd.count())
      rdd.reduceByKey((y,z)=>y+z).foreach(x=>println(x._1 + " : " + x._2))
      println()
    })



    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}


