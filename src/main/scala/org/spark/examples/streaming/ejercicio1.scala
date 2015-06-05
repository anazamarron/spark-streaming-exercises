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
object ejercicio1 {

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
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ReadingLogs_exercise")
    //Define a SparkStreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //Connect to a socket and read a text stream
    val autorizacion = ssc.socketTextStream("localhost", 10001, StorageLevel.MEMORY_AND_DISK_SER)
    //Filter out empty lines and print the count
    val numberEvents_authRDD = autorizacion.map(x => {
      val arr = x.split(';')
      new AuthEvent(arr(0), arr(1), arr(2), arr(3))
    })


    val sucess_auth_eventsRDD = numberEvents_authRDD.filter(linea => linea.Message.contains("OK"))
    val no_sucess_auth_eventsRDD = numberEvents_authRDD.filter(linea => linea.Message.contains("failed"))
    //.reduceByKey((x,y) => x+y).foreachRDD(x=>println("Number of request:" + x._1))
    //Print 10 of them


    val events = ssc.socketTextStream("localhost", 10002, StorageLevel.MEMORY_AND_DISK_SER)
    //Filter out empty lines and print the count
    val numberEventsRDD = events.map(x => {
      val arr = x.split(';')
      new WebEvent(arr(0), arr(1), arr(2), arr(3), arr(4))
    })

    val total_wr = numberEventsRDD.foreachRDD(x => x.count)
    // val mapped_events_RDD = numberEventsRDD.map(x=>(x.sourceHost))
    //Print 10 of them

    //- Number of authentication events.

    val sucess_wr_eventsRDD = numberEventsRDD.filter(linea => linea.HTTP_code.contains("200"))
    val no_sucess_wr_eventsRDD = numberEventsRDD.filter(linea => linea.HTTP_code.contains("402"))
    //.re
    println("")
    println("Ejercicio 1: ")
    println("**********************************************")
    println("")
    val tot_aut = numberEvents_authRDD.foreachRDD(aut => aut.count)
    numberEvents_authRDD.foreachRDD(aut => println(" Num autorizaciones: " + aut.count))
    numberEventsRDD.foreachRDD(wr => println(" Number of web requests: " + wr.count))
    sucess_auth_eventsRDD.foreachRDD(sa => println(" Number of successful authentication event: " + sa.count))
    no_sucess_auth_eventsRDD.foreachRDD(sa => println(" Number of failed authentication event: " + sa.count))
    sucess_wr_eventsRDD.foreachRDD(sa => println(" Number of successful web requests: " + sa.count))
    no_sucess_wr_eventsRDD.foreachRDD(sa => println( "Number of failed web requests: " + sa.count))
    println("")

   // numberEventsRDD.print(100)

    println("")
    println("Ejercicio 2: ")
    println("**********************************************")
    println("")


    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}


