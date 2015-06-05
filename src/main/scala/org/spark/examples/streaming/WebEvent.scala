package org.spark.examples.streaming

/**
 * Web Event.
 */


case class WebEvent(sourceHost: String
                    , timestamp: String
                    , method: String
                    , URL: String
                    , HTTP_code:String) {

}

object WebEvent {

}


//case class WebEvent(...)