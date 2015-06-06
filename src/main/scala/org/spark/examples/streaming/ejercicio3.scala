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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Code here the solution for the proposed exercises.
 */
object ejercicio3 {

  /**
   * Field separator.
   */
  val Separator = ";";

  /**
   * Threshold that determines when a number of failed auth entries is considered an attack.
   */
  val ThresholdAuth = 18;

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

   //Voy a usar el de las autorizaciones. hago un print para que me de los tipos de lineas

    // Given a window of 10 seconds and the auth stream, determine which hosts could be attacking the system.
    // In order to do  that, aggregate the hosts counting the number of failed requests and filter those that
    // have at least ThresholdAuth requests.

    val autorizacion = ssc.socketTextStream("localhost", 10001, StorageLevel.MEMORY_AND_DISK_SER)
    val numberEvents_authRDD = autorizacion.map(x => {
      val arr = x.split(';')
      new AuthEvent(arr(0), arr(1), arr(2), arr(3))
    })

    val filteredRDD = numberEvents_authRDD.foreachRDD(rdds=>{

      //primero filtro los que son ataque y los sumo
      val comunRDD =rdds.filter(aut=>aut.Message.contains("failed"))
        .map(x=>(x.Source,1))
        .reduceByKey((acum,nuevo)=>acum+nuevo)

      // Imprimo el total de los ataques por cada servidor
      comunRDD.foreach(host=>println("Número de ataques Total : " + host._1,host._2))

      // ahora con el ThresholdAuth que lo he subido a 18 para que no me
      // devuelva todos los servidores

      comunRDD.filter(num => num._2>=ThresholdAuth)
        .foreach(host=>println("Num de ataques que superan el umbral: " + host._1,host._2))

      println("")

    })



    /*
        numberEvents_authRDD.foreachRDD(x=>x.take(1).map(x=>{
          println("llamada ")
          println("*********")
          println("Mensaje: " + x.Message)
          println("Process: " + x.Process)
          println("Source: " + x.Source)
          println("Timestamp: " + x.Timestamp)
          println("")
        }
        ))
        */





    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}


