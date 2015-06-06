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
object ejercicio6 {

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
    ssc.checkpoint("cache/") //Que deje el checkpoint en el directorio caché para no llenar el raiz


    val events = ssc.socketTextStream("localhost", 10002, StorageLevel.MEMORY_AND_DISK_SER)

    val numberEventsRDD = events.map(x => {
      val arr = x.split(';')
      new WebEvent(arr(0), arr(1), arr(2), arr(3), arr(4))
    })



    // Join both auth and web streams to determine the hosts receiving the highest number of successful requests, and the
    // hosts with the higher number of attacks. Use a window of 10 seconds.

    val autorizacion = ssc.socketTextStream("localhost", 10001, StorageLevel.MEMORY_AND_DISK_SER)

    val numberEvents_authRDD = autorizacion.map(x => {
      val arr = x.split(';')
      new AuthEvent(arr(0), arr(1), arr(2), arr(3))
    })


    //Esta funcion es la que voy a usar para acumular el resultado de cada uno de los streams
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //Obtengo la fecha del sistema para imprimirla

    val today = Calendar.getInstance().getTime()

    //Obtengo los eventos de éxito de ambos host

    val autOkRDD = numberEvents_authRDD
        .filter(aut => aut.Message.contains("OK"))
        .map(x => (x.Source, 1))
        .reduceByKey((y,z) => y+z)
        .map(x=>(x._1,x._2))

    val evOKRDD = numberEventsRDD
      .filter(aut => aut.HTTP_code.contains("200"))
      .map(x => (x.sourceHost, 1))
      .reduceByKey((y,z) => y+z)
      .map(x=>(x._1,x._2))

    val totalOKRDD = autOkRDD.join(evOKRDD).map(x => (x._1,x._2._1+x._2._2))


    //obtengo los eventos fallidos de cada host

    val autKORDD = numberEvents_authRDD
      .filter(aut => aut.Message.contains("failed"))
      .map(x => (x.Source, 1))
      .reduceByKey((y,z) => y+z)
      .map(x=>(x._1,x._2))

    val evKORDD = numberEventsRDD
      .filter(aut => aut.HTTP_code.contains("402"))
      .map(x => (x.sourceHost, 1))
      .reduceByKey((y,z) => y+z)
      .map(x=>(x._1,x._2))

    val totalKORDD = autKORDD.join(evKORDD).map(x => (x._1,x._2._1+x._2._2))

    val totalGeneral = totalOKRDD.join(totalKORDD).map(x=>(x._1,(x._2._1,x._2._2)))

    totalGeneral.foreachRDD(rdd=>{
      println()
      println(today + ":")
      rdd.sortBy(x=>x._2._1,false).take(3).foreach(x=>println("OK: " + x._1 + " : " + x._2._1))
      println()
      rdd.sortBy(x=>x._2._2,false).take(3).foreach(x=>println("KO: " + x._1 + " : " + x._2._2))
    })

    //Hago Join de los eventos de Éxito + los eventos fallidos



    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}


