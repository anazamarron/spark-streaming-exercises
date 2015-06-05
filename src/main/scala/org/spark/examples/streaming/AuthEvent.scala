package org.spark.examples.streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Auth Event.
 */
case class AuthEvent(Timestamp: String
                     , Source: String
                     , Process: String
                     , Message:String) {

}

object AuthEvent {

}

//case class AuthEvent(...)

